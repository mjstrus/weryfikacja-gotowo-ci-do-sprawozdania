"""
=============================================================================
SymfoniaYearEndAuditor – Automatyczna Kontrola Jakości Danych Finansowych
Biuro Rachunkowe Abacus | Zamknięcie Roku Obrachunkowego
=============================================================================
Wersja 2.0 – obsługa wielu rachunków bankowych (analityki konta 130)
            + wykrywanie miesiąca ostatniego wyciągu

Moduł weryfikuje spójność danych między:
  - ZOiS (Zestawienie Obrotów i Sald) – źródło główne
  - Bilansem (Aktywa = Pasywa)
  - Wyciągami Bankowymi (1..N rachunków, konto 130-X)

Kompatybilny z Frappe Framework (Server Script / Python Background Job).
Przetwarza dane wyłącznie w pamięci (in-memory) – brak zapisu plików tymczasowych.
=============================================================================
"""

from __future__ import annotations

import io
import re
import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

# ── Opcjonalne biblioteki ─────────────────────────────────────────────────────
try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

try:
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

logger = logging.getLogger("SymfoniaAuditor")
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# Polskie nazwy miesięcy
MIESIACE_PL = {
    1: "styczeń", 2: "luty", 3: "marzec", 4: "kwiecień",
    5: "maj", 6: "czerwiec", 7: "lipiec", 8: "sierpień",
    9: "wrzesień", 10: "październik", 11: "listopad", 12: "grudzień",
}


# =============================================================================
# TYPY
# =============================================================================

class StatusAudytu(Enum):
    OK       = "✅ OK"
    BLAD     = "❌ BŁĄD"
    OSTRZEZ  = "⚠️  OSTRZEŻENIE"
    BRAK     = "🔍 BRAK DANYCH"
    INFO     = "ℹ️  INFO"


@dataclass
class PunktKontroli:
    konto:   str
    punkt:   str
    status:  StatusAudytu
    uwagi:   str = ""
    wartosc: str = ""


@dataclass
class DaneZOiS:
    """
    Zestawienie Obrotów i Sald sparsowane z Symfonii.
    Przechowuje DWA widoki:
      - konta:           agregacja do syntetyki (np. "400", "401", "130")
      - konta_analityki: pełne numery analityczne (np. "130-1", "130-2")
    """
    konta: Dict[str, Tuple[Decimal, Decimal]] = field(default_factory=dict)
    konta_analityki: Dict[str, Tuple[Decimal, Decimal]] = field(default_factory=dict)
    opisy: Dict[str, str] = field(default_factory=dict)

    def pobierz_konta_bankowe(self) -> List[Tuple[str, str, Decimal]]:
        """
        Zwraca listę WSZYSTKICH rachunków bankowych z ZOiS (analityki konta 130).

        Returns:
            Lista: (numer_ksiegowy, opis, saldo_netto) posortowana po numerze.
        """
        wynik = []
        for numer, (wn, ma) in self.konta_analityki.items():
            if normalize_konto(numer) == "130":
                saldo_netto = wn - ma
                opis = self.opisy.get(numer, "Rachunek bankowy")
                wynik.append((numer, opis, saldo_netto))

        # Fallback – brak analityk, tylko syntetyka
        if not wynik and "130" in self.konta:
            wn, ma = self.konta["130"]
            wynik.append(("130", self.opisy.get("130", "Rachunek bankowy"), wn - ma))

        return sorted(wynik, key=lambda x: x[0])


@dataclass
class DaneBilansu:
    aktywa_biezacy: Decimal = Decimal("0")
    pasywa_biezacy: Decimal = Decimal("0")
    aktywa_ubiegly: Decimal = Decimal("0")
    pasywa_ubiegly: Decimal = Decimal("0")


@dataclass
class WyciagBankowy:
    """
    Pojedynczy wyciąg bankowy powiązany z konkretną analityką konta 130.
    """
    numer_konta_ksiegowego: str           # np. "130-1"
    saldo_koncowe: Decimal
    rok_ostatniej_operacji: Optional[int] = None
    miesiac_ostatniej_operacji: Optional[int] = None
    numer_rachunku: str = ""
    bank_nazwa: str = ""
    wgrany_plik: bool = True

    @property
    def okres_opisowy(self) -> str:
        """Np. 'grudzień 2024' lub 'kwiecień 2024'."""
        if self.miesiac_ostatniej_operacji and self.rok_ostatniej_operacji:
            nazwa = MIESIACE_PL.get(self.miesiac_ostatniej_operacji, "?")
            return f"{nazwa} {self.rok_ostatniej_operacji}"
        elif self.rok_ostatniej_operacji:
            return str(self.rok_ostatniej_operacji)
        return "okres nieznany"


# =============================================================================
# FUNKCJE NORMALIZACJI
# =============================================================================

def normalize_currency(wartosc: Union[str, float, int, None]) -> Decimal:
    """Normalizuje kwotę finansową do Decimal. Obsługuje formaty PL i EN."""
    if wartosc is None:
        return Decimal("0")

    if isinstance(wartosc, (int, float)):
        try:
            return Decimal(str(round(float(wartosc), 2)))
        except (InvalidOperation, ValueError):
            return Decimal("0")

    tekst = str(wartosc).strip()
    if not tekst or tekst in ("-", "–", "0,00", "0.00"):
        return Decimal("0")

    tekst = re.sub(r"[złZŁPLN\xa0]", "", tekst).strip()

    if "," in tekst and "." in tekst:
        if tekst.rfind(",") > tekst.rfind("."):
            tekst = tekst.replace(".", "").replace(",", ".")
        else:
            tekst = tekst.replace(",", "")
    elif "," in tekst:
        czesci = tekst.split(",")
        if len(czesci) == 2 and len(czesci[1]) <= 2:
            tekst = tekst.replace(",", ".")
        else:
            tekst = tekst.replace(",", "")

    tekst = tekst.replace(" ", "")

    try:
        return Decimal(tekst).quantize(Decimal("0.01"))
    except InvalidOperation:
        raise ValueError(f"Nie można znormalizować kwoty: '{wartosc}'")


def normalize_konto(numer: str) -> str:
    """Normalizuje numer konta do poziomu syntetyki. Np. '201-1-001' → '201'."""
    if not numer:
        return ""
    return str(numer).strip().split("-")[0].strip()


def get_grupa(numer_konta: str) -> Optional[int]:
    """Zwraca numer grupy konta (pierwsza cyfra)."""
    syntetyka = normalize_konto(numer_konta)
    if syntetyka and syntetyka[0].isdigit():
        return int(syntetyka[0])
    return None


def wykryj_date_ostatniej_operacji(tekst: str) -> Optional[Tuple[int, int]]:
    """
    Heurystycznie wykrywa rok i miesiąc ostatniej operacji w wyciągu bankowym.

    Szuka wszystkich dat w tekście i zwraca najpóźniejszą.
    Obsługuje formaty: DD.MM.YYYY, DD-MM-YYYY, YYYY-MM-DD, MM/YYYY.

    Returns:
        Krotka (rok, miesiąc) lub None jeśli nic nie znaleziono.
    """
    wszystkie_daty: List[date] = []

    wzorce = [
        r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})",   # 31.12.2024
        r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})",   # 2024-12-31
    ]

    for wzorzec in wzorce:
        for m in re.finditer(wzorzec, tekst):
            g = m.groups()
            try:
                if len(g[0]) == 4:
                    rok, mies, dzien = int(g[0]), int(g[1]), int(g[2])
                else:
                    dzien, mies, rok = int(g[0]), int(g[1]), int(g[2])
                if 1 <= mies <= 12 and 1 <= dzien <= 31 and 2000 <= rok <= 2100:
                    wszystkie_daty.append(date(rok, mies, dzien))
            except (ValueError, IndexError):
                continue

    # MM/YYYY – numer wyciągu
    for m in re.finditer(r"\b(\d{1,2})/(\d{4})\b", tekst):
        try:
            mies, rok = int(m.group(1)), int(m.group(2))
            if 1 <= mies <= 12 and 2000 <= rok <= 2100:
                wszystkie_daty.append(date(rok, mies, 1))
        except ValueError:
            continue

    if not wszystkie_daty:
        return None

    najpozniejsza = max(wszystkie_daty)
    return (najpozniejsza.year, najpozniejsza.month)


# =============================================================================
# PARSER ZOiS
# =============================================================================

class ParserZOiS:
    """Parser ZOiS – zachowuje zarówno syntetyki jak i analityki."""

    KOLUMNY_KONTO = ["konto", "numer konta", "nr konta", "symbol konta", "account"]
    KOLUMNY_NAZWA = ["nazwa", "nazwa konta", "opis", "name"]
    KOLUMNY_SALDO_WN = ["saldo wn", "saldo_wn", "debet", "wn", "saldo dt", "dt"]
    KOLUMNY_SALDO_MA = ["saldo ma", "saldo_ma", "kredyt", "ma", "saldo ct", "ct"]

    def _znajdz_kolumne(self, df, kandydaci):
        kol_lower = {k.lower().strip(): k for k in df.columns}
        for k in kandydaci:
            if k.lower() in kol_lower:
                return kol_lower[k.lower()]
        return None

    def parsuj_xlsx(self, dane_binarne: bytes) -> DaneZOiS:
        bufor = io.BytesIO(dane_binarne)
        try:
            wb = openpyxl.load_workbook(bufor, data_only=True)
        except Exception as e:
            raise ValueError(f"Błąd odczytu XLSX (ZOiS): {e}")

        arkusz = wb.active
        for nazwa in ["ZOiS", "Zestawienie", "ObrotyiSalda", "Sheet1"]:
            if nazwa in wb.sheetnames:
                arkusz = wb[nazwa]
                break

        wiersze = [list(w) for w in arkusz.iter_rows(values_only=True)]
        if not wiersze:
            raise ValueError("Arkusz ZOiS jest pusty.")

        idx = self._znajdz_wiersz_naglowka(wiersze)
        if idx is None:
            raise ValueError("Nie znaleziono wiersza nagłówkowego w ZOiS.")

        naglowki = [str(k).strip() if k else "" for k in wiersze[idx]]
        df = pd.DataFrame(wiersze[idx+1:], columns=naglowki)
        return self._parsuj_dataframe(df, "XLSX")

    def _znajdz_wiersz_naglowka(self, wiersze):
        kluczowe = {"konto", "saldo", "wn", "ma", "obroty", "numer"}
        for i, w in enumerate(wiersze[:20]):
            vals = {str(v).lower().strip() for v in w if v is not None}
            if len(vals & kluczowe) >= 2:
                return i
        return None

    def parsuj_pdf(self, dane_binarne: bytes) -> DaneZOiS:
        if not PDF_AVAILABLE:
            raise ImportError("Brak biblioteki pdfplumber.")

        bufor = io.BytesIO(dane_binarne)
        wiersze = []
        naglowki = None

        with pdfplumber.open(bufor) as pdf:
            for strona in pdf.pages:
                tabele = strona.extract_tables(table_settings={
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "snap_tolerance": 3,
                })
                for tabela in tabele or []:
                    for w in tabela or []:
                        if w is None:
                            continue
                        vals = [str(v or "").strip() for v in w]
                        if naglowki is None:
                            wl = {v.lower() for v in vals}
                            if len(wl & {"konto", "saldo", "wn", "ma"}) >= 2:
                                naglowki = vals
                                continue
                        if naglowki is not None and any(vals):
                            wiersze.append(vals)

        if naglowki is None or not wiersze:
            raise ValueError("Nie znaleziono tabeli ZOiS w PDF.")

        df = pd.DataFrame(wiersze, columns=naglowki)
        return self._parsuj_dataframe(df, "PDF")

    def _parsuj_dataframe(self, df: pd.DataFrame, zrodlo: str) -> DaneZOiS:
        kol_konto = self._znajdz_kolumne(df, self.KOLUMNY_KONTO)
        kol_nazwa = self._znajdz_kolumne(df, self.KOLUMNY_NAZWA)
        kol_wn    = self._znajdz_kolumne(df, self.KOLUMNY_SALDO_WN)
        kol_ma    = self._znajdz_kolumne(df, self.KOLUMNY_SALDO_MA)

        brakujace = []
        if not kol_konto: brakujace.append("Konto")
        if not kol_wn:    brakujace.append("Saldo Wn")
        if not kol_ma:    brakujace.append("Saldo Ma")
        if brakujace:
            raise ValueError(
                f"Brak kolumn ZOiS ({zrodlo}): {', '.join(brakujace)}. "
                f"Dostępne: {list(df.columns)}"
            )

        wynik = DaneZOiS()

        for _, w in df.iterrows():
            numer_raw = w.get(kol_konto)
            if pd.isna(numer_raw) or not str(numer_raw).strip():
                continue

            numer_pelny = str(numer_raw).strip()
            numer_syn = normalize_konto(numer_pelny)

            if numer_syn.lower() in {"razem", "suma", "ogółem", "total"}:
                continue
            if not re.match(r"^\d+", numer_syn):
                continue

            try:
                wn = normalize_currency(w.get(kol_wn))
                ma = normalize_currency(w.get(kol_ma))
            except ValueError as e:
                logger.warning(f"Pominięto {numer_pelny}: {e}")
                continue

            # Zapis analityki (pełny numer – potrzebny dla konta 130-X)
            if numer_pelny != numer_syn:
                iwn, ima = wynik.konta_analityki.get(numer_pelny, (Decimal("0"), Decimal("0")))
                wynik.konta_analityki[numer_pelny] = (iwn + wn, ima + ma)
                if kol_nazwa:
                    opis = w.get(kol_nazwa)
                    if not pd.isna(opis) and numer_pelny not in wynik.opisy:
                        wynik.opisy[numer_pelny] = str(opis).strip()

            # Agregacja do syntetyki
            iwn, ima = wynik.konta.get(numer_syn, (Decimal("0"), Decimal("0")))
            wynik.konta[numer_syn] = (iwn + wn, ima + ma)

            if kol_nazwa and numer_syn not in wynik.opisy:
                opis = w.get(kol_nazwa)
                if not pd.isna(opis):
                    wynik.opisy[numer_syn] = str(opis).strip()

        # Fallback: syntetyka 130 bez analityk
        if "130" in wynik.konta and not any(
            normalize_konto(k) == "130" for k in wynik.konta_analityki
        ):
            wynik.konta_analityki["130"] = wynik.konta["130"]

        logger.info(
            f"ZOiS ({zrodlo}): {len(wynik.konta)} syntetyk, "
            f"{len(wynik.konta_analityki)} analityk."
        )
        return wynik


# =============================================================================
# PARSER BILANSU
# =============================================================================

class ParserBilansu:
    KLUCZE_AKTYWA = ["suma aktywów", "aktywa razem", "total assets", "a k t y w a"]
    KLUCZE_PASYWA = ["suma pasywów", "pasywa razem", "total liabilities", "p a s y w a"]

    def parsuj_xlsx(self, dane_binarne: bytes) -> DaneBilansu:
        bufor = io.BytesIO(dane_binarne)
        try:
            wb = openpyxl.load_workbook(bufor, data_only=True)
            wiersze = list(wb.active.iter_rows(values_only=True))
        except Exception as e:
            raise ValueError(f"Błąd odczytu XLSX (Bilans): {e}")
        return self._szukaj_sum(wiersze)

    def parsuj_pdf(self, dane_binarne: bytes) -> DaneBilansu:
        if not PDF_AVAILABLE:
            raise ImportError("Brak pdfplumber.")
        bufor = io.BytesIO(dane_binarne)
        wszystkie = []
        with pdfplumber.open(bufor) as pdf:
            for strona in pdf.pages:
                t = strona.extract_text() or ""
                wszystkie.extend(t.splitlines())
        return self._szukaj_sum([[w] for w in wszystkie])

    def _szukaj_sum(self, wiersze) -> DaneBilansu:
        wynik = DaneBilansu()
        for w in wiersze:
            if not w:
                continue
            t = " ".join(str(k).lower().strip() for k in w if k is not None)

            if any(kl in t for kl in self.KLUCZE_AKTYWA):
                kw = self._wyciagnij_kwoty(w)
                if kw:
                    wynik.aktywa_biezacy = kw[0]
                    wynik.aktywa_ubiegly = kw[1] if len(kw) > 1 else Decimal("0")
            elif any(kl in t for kl in self.KLUCZE_PASYWA):
                kw = self._wyciagnij_kwoty(w)
                if kw:
                    wynik.pasywa_biezacy = kw[0]
                    wynik.pasywa_ubiegly = kw[1] if len(kw) > 1 else Decimal("0")
        return wynik

    def _wyciagnij_kwoty(self, wiersz):
        kwoty = []
        for k in wiersz:
            if k is None: continue
            try:
                n = normalize_currency(k)
                if n != Decimal("0"):
                    kwoty.append(n)
            except ValueError:
                continue
        return kwoty


# =============================================================================
# PARSER WYCIĄGU BANKOWEGO (v2.0 – wykrywanie daty ostatniej operacji)
# =============================================================================

class ParserWyciaguBankowego:
    """Parser wyciągu – ekstrahuje saldo, datę ostatniej operacji, nazwę banku."""

    KLUCZE_SALDO = [
        "saldo końcowe", "saldo na koniec", "closing balance",
        "stan na koniec", "saldo końca okresu",
    ]

    WZORZEC_IBAN = re.compile(r"(?:PL)?\s*(\d{2}\s*(?:\d{4}\s*){6})")

    ZNANE_BANKI = [
        "Santander", "PKO BP", "PKO Bank", "mBank", "ING Bank", "Pekao",
        "BNP Paribas", "Millennium", "Credit Agricole", "Alior", "Citi",
        "Getin", "Nest Bank", "Raiffeisen", "Deutsche Bank", "BOŚ",
    ]

    def parsuj(
        self, dane_binarne: bytes, format_pliku: str
    ) -> Tuple[Decimal, Optional[int], Optional[int], str, str]:
        """
        Returns: (saldo_koncowe, rok, miesiac, numer_rachunku, nazwa_banku)
        """
        if format_pliku.lower() == "pdf":
            return self._parsuj_pdf(dane_binarne)
        return self._parsuj_xlsx(dane_binarne)

    def _parsuj_pdf(self, dane_binarne: bytes):
        if not PDF_AVAILABLE:
            raise ImportError("Brak pdfplumber.")

        bufor = io.BytesIO(dane_binarne)
        saldo = Decimal("0")
        caly_tekst = ""

        with pdfplumber.open(bufor) as pdf:
            for strona in pdf.pages:
                caly_tekst += (strona.extract_text() or "") + "\n"

            for strona in reversed(pdf.pages):
                t = strona.extract_text() or ""
                s = self._wyciagnij_saldo(t)
                if s and s != Decimal("0"):
                    saldo = s
                    break

        data_op = wykryj_date_ostatniej_operacji(caly_tekst)
        rok, mies = data_op if data_op else (None, None)
        return saldo, rok, mies, self._iban(caly_tekst), self._bank(caly_tekst)

    def _parsuj_xlsx(self, dane_binarne: bytes):
        bufor = io.BytesIO(dane_binarne)
        try:
            df = pd.read_excel(bufor, engine="openpyxl", header=None)
        except Exception as e:
            raise ValueError(f"Błąd odczytu XLSX wyciągu: {e}")

        saldo = Decimal("0")
        caly_tekst = ""

        for _, wiersz in df.iterrows():
            for kom in wiersz:
                if isinstance(kom, str):
                    caly_tekst += kom + " "
                    if any(k in kom.lower() for k in self.KLUCZE_SALDO):
                        for v in wiersz:
                            try:
                                kw = normalize_currency(v)
                                if kw != Decimal("0"):
                                    saldo = kw
                                    break
                            except (ValueError, TypeError):
                                pass
                elif kom is not None:
                    caly_tekst += str(kom) + " "

        data_op = wykryj_date_ostatniej_operacji(caly_tekst)
        rok, mies = data_op if data_op else (None, None)
        return saldo, rok, mies, self._iban(caly_tekst), self._bank(caly_tekst)

    def _wyciagnij_saldo(self, tekst: str) -> Optional[Decimal]:
        for linia in tekst.splitlines():
            ll = linia.lower()
            if any(k in ll for k in self.KLUCZE_SALDO):
                for token in reversed(linia.split()):
                    try:
                        kw = normalize_currency(token)
                        if kw != Decimal("0"):
                            return kw
                    except (ValueError, TypeError):
                        pass
        return None

    def _iban(self, tekst: str) -> str:
        m = self.WZORZEC_IBAN.search(tekst)
        return re.sub(r"\s+", "", m.group(1)) if m else ""

    def _bank(self, tekst: str) -> str:
        tl = tekst.lower()
        for bank in self.ZNANE_BANKI:
            if bank.lower() in tl:
                return bank
        return ""


# =============================================================================
# GŁÓWNA KLASA AUDYTORA
# =============================================================================

class SymfoniaYearEndAuditor:
    """
    Audytor jakości danych – zamknięcie roku.

    API:
        parsuj_zois(bytes, format)    → DaneZOiS (etap 1)
        parsuj_wyciag(konto, bytes, format) → WyciagBankowy (etap 2, per rachunek)
        check_accounting_logic(...)   → weryfikacja
        generate_audit_report(...)    → raport
        run_full_audit(...)           → wszystko naraz
    """

    def __init__(self):
        self._parser_zois   = ParserZOiS()
        self._parser_bilans = ParserBilansu()
        self._parser_wyciag = ParserWyciaguBankowego()
        self._wyniki: List[PunktKontroli] = []

    # ─── Parsowanie pojedynczych źródeł (dla UI dwuetapowego) ────────────────

    def parsuj_zois(self, dane_binarne: bytes, format_pliku: str = "xlsx") -> DaneZOiS:
        if format_pliku.lower() == "pdf":
            return self._parser_zois.parsuj_pdf(dane_binarne)
        return self._parser_zois.parsuj_xlsx(dane_binarne)

    def parsuj_bilans(self, dane_binarne: bytes, format_pliku: str = "xlsx") -> DaneBilansu:
        if format_pliku.lower() == "pdf":
            return self._parser_bilans.parsuj_pdf(dane_binarne)
        return self._parser_bilans.parsuj_xlsx(dane_binarne)

    def parsuj_wyciag(
        self,
        numer_konta_ksiegowego: str,
        dane_binarne: bytes,
        format_pliku: str = "pdf",
    ) -> WyciagBankowy:
        """Parsuje wyciąg bankowy dowiązując do konkretnej analityki 130-X."""
        saldo, rok, mies, iban, bank = self._parser_wyciag.parsuj(
            dane_binarne, format_pliku
        )
        return WyciagBankowy(
            numer_konta_ksiegowego=numer_konta_ksiegowego,
            saldo_koncowe=saldo,
            rok_ostatniej_operacji=rok,
            miesiac_ostatniej_operacji=mies,
            numer_rachunku=iban,
            bank_nazwa=bank,
            wgrany_plik=True,
        )

    # ─── Pełna weryfikacja ───────────────────────────────────────────────────

    def check_accounting_logic(
        self,
        dane_zois:   Optional[DaneZOiS],
        dane_bilans: Optional[DaneBilansu],
        wyciagi:     Optional[List[WyciagBankowy]] = None,
        rok_obrachunkowy: int = 2024,
    ) -> List[PunktKontroli]:
        self._wyniki = []
        wyciagi = wyciagi or []

        if dane_zois is None:
            self._wyniki.append(PunktKontroli(
                konto="ZOiS", punkt="Wczytanie ZOiS", status=StatusAudytu.BRAK,
                uwagi="Nie dostarczono pliku ZOiS.",
            ))
        else:
            self._weryfikuj_konta_bankowe(dane_zois, wyciagi, rok_obrachunkowy)
            self._weryfikuj_konto_145(dane_zois)
            self._weryfikuj_konto_200(dane_zois)
            self._weryfikuj_konto_202(dane_zois)
            self._weryfikuj_konto_230(dane_zois)
            self._weryfikuj_konto_229(dane_zois)
            self._weryfikuj_konto_220(dane_zois)
            self._weryfikuj_konto_700(dane_zois)
            self._weryfikuj_grupe_4(dane_zois)

        self._weryfikuj_bilans(dane_bilans)
        return self._wyniki

    def generate_audit_report(
        self,
        wyniki: List[PunktKontroli],
        nazwa_podmiotu: str = "Podmiot",
        rok: int = 2024,
    ) -> Dict:
        linia = "═" * 70
        L = [
            linia,
            f"  RAPORT KONTROLI JAKOŚCI DANYCH – ZAMKNIĘCIE ROKU {rok}",
            f"  Podmiot: {nazwa_podmiotu}",
            f"  Wygenerowano przez: SymfoniaYearEndAuditor v2.0",
            linia, "",
            f"{'KONTO':<14} {'STATUS':<20} {'PUNKT KONTROLI':<42} UWAGI",
            "─" * 110,
        ]

        stats = {s: 0 for s in StatusAudytu}
        wyniki_slow = []

        for pkt in wyniki:
            stats[pkt.status] += 1
            wartosc = f" [{pkt.wartosc}]" if pkt.wartosc else ""
            L.append(
                f"{pkt.konto:<14} {pkt.status.value:<20} "
                f"{pkt.punkt:<42} {pkt.uwagi}{wartosc}"
            )
            wyniki_slow.append({
                "konto": pkt.konto, "status": pkt.status.value,
                "punkt": pkt.punkt, "uwagi": pkt.uwagi, "wartosc": pkt.wartosc,
            })

        bledy   = [p for p in wyniki if p.status == StatusAudytu.BLAD]
        ostrz   = [p for p in wyniki if p.status == StatusAudytu.OSTRZEZ]
        brak    = [p for p in wyniki if p.status == StatusAudytu.BRAK]

        L.extend(["", linia, "  PODSUMOWANIE", linia])
        L.append(f"  ✅ OK:            {stats[StatusAudytu.OK]}")
        L.append(f"  ❌ BŁĘDY:         {stats[StatusAudytu.BLAD]}")
        L.append(f"  ⚠️  OSTRZEŻENIA:  {stats[StatusAudytu.OSTRZEZ]}")
        L.append(f"  🔍 BRAK DANYCH:  {stats[StatusAudytu.BRAK]}")

        if bledy:
            L.extend(["", "  ❌ WYMAGANE DZIAŁANIA (BŁĘDY KRYTYCZNE):"])
            for p in bledy:
                L.append(f"    → {p.konto}: {p.uwagi}")
        if ostrz:
            L.extend(["", "  ⚠️  DO WYJAŚNIENIA (OSTRZEŻENIA):"])
            for p in ostrz:
                L.append(f"    → {p.konto}: {p.uwagi}")
        if brak:
            L.extend(["", "  🔍 BRAKI DANYCH:"])
            for p in brak:
                L.append(f"    → {p.konto}: {p.uwagi}")

        L.append("")
        if stats[StatusAudytu.BLAD] == 0 and stats[StatusAudytu.OSTRZEZ] == 0:
            L.append("  🎉 OCENA KOŃCOWA: DANE SPÓJNE – gotowe do badania.")
        elif stats[StatusAudytu.BLAD] == 0:
            L.append("  🟡 OCENA KOŃCOWA: WYMAGA WYJAŚNIENIA.")
        else:
            L.append("  🔴 OCENA KOŃCOWA: DANE NIESPÓJNE – wymagane korekty.")
        L.append(linia)

        return {
            "tekst": "\n".join(L),
            "wyniki": wyniki_slow,
            "podsumowanie": {
                "ok": stats[StatusAudytu.OK],
                "bledy": stats[StatusAudytu.BLAD],
                "ostrzezenia": stats[StatusAudytu.OSTRZEZ],
                "brak_danych": stats[StatusAudytu.BRAK],
                "podmiot": nazwa_podmiotu,
                "rok": rok,
            },
        }

    def run_full_audit(
        self,
        *,
        zois_bytes: Optional[bytes] = None,
        zois_format: str = "xlsx",
        bilans_bytes: Optional[bytes] = None,
        bilans_format: str = "xlsx",
        wyciagi: Optional[List[WyciagBankowy]] = None,
        nazwa_podmiotu: str = "Podmiot",
        rok: int = 2024,
    ) -> Dict:
        dane_zois   = self.parsuj_zois(zois_bytes, zois_format) if zois_bytes else None
        dane_bilans = self.parsuj_bilans(bilans_bytes, bilans_format) if bilans_bytes else None

        wyniki = self.check_accounting_logic(
            dane_zois, dane_bilans, wyciagi or [], rok_obrachunkowy=rok,
        )
        return self.generate_audit_report(wyniki, nazwa_podmiotu, rok)

    # ─── WERYFIKACJA RACHUNKÓW BANKOWYCH (v2.0 – wieloraczkowe) ──────────────

    def _weryfikuj_konta_bankowe(
        self,
        dane_zois: DaneZOiS,
        wyciagi: List[WyciagBankowy],
        rok_obrachunkowy: int,
    ) -> None:
        """
        Weryfikuje WSZYSTKIE rachunki bankowe (analityki konta 130) z ZOiS.

        Dla każdego rachunku:
          1. Sprawdza czy wgrano wyciąg
          2. Porównuje saldo
          3. Informuje o miesiącu ostatniej operacji (ostrzeżenie jeśli nie XII)

        Na końcu – podsumowanie rachunków BEZ wgranego wyciągu.
        """
        konta_bankowe = dane_zois.pobierz_konta_bankowe()

        if not konta_bankowe:
            self._wyniki.append(PunktKontroli(
                konto="130", punkt="Rachunki bankowe",
                status=StatusAudytu.BRAK,
                uwagi="CRITICAL: Nie odnaleziono konta 130 w ZOiS.",
            ))
            return

        mapa = {w.numer_konta_ksiegowego: w for w in wyciagi}

        # Info wprowadzająca
        self._wyniki.append(PunktKontroli(
            konto="130", punkt="Wykryte rachunki bankowe",
            status=StatusAudytu.INFO,
            uwagi=(
                f"Wykryto {len(konta_bankowe)} rachunek(ów) w ZOiS, "
                f"wgrano {len(wyciagi)} wyciąg(ów)."
            ),
        ))

        rachunki_bez_wyciagu: List[Tuple[str, str, Decimal]] = []

        for numer_ks, opis, saldo_zois in konta_bankowe:
            wyciag = mapa.get(numer_ks)

            if wyciag is None:
                rachunki_bez_wyciagu.append((numer_ks, opis, saldo_zois))
                self._wyniki.append(PunktKontroli(
                    konto=numer_ks,
                    punkt=f"Rachunek {opis}",
                    status=StatusAudytu.BRAK,
                    uwagi=(
                        f"Brak wgranego wyciągu. "
                        f"Saldo ZOiS: {saldo_zois:,.2f} zł. "
                        "Uzupełnij aby zweryfikować zgodność."
                    ),
                ))
                continue

            roznica = abs(saldo_zois - wyciag.saldo_koncowe)

            # Informacja o okresie wyciągu
            okres_info = ""
            ostrz_okresu = ""
            if wyciag.miesiac_ostatniej_operacji:
                okres_info = f" | Ostatnia operacja: {wyciag.okres_opisowy}"
                if (wyciag.miesiac_ostatniej_operacji != 12 or
                    (wyciag.rok_ostatniej_operacji and
                     wyciag.rok_ostatniej_operacji != rok_obrachunkowy)):
                    ostrz_okresu = (
                        f" UWAGA: Rachunek zamknięty operacjami z "
                        f"{wyciag.okres_opisowy}, nie z grudnia {rok_obrachunkowy}."
                    )

            bank_str = f" ({wyciag.bank_nazwa})" if wyciag.bank_nazwa else ""

            if roznica < Decimal("0.01"):
                # Saldo zgodne – ale jeśli okres nie grudzień, OSTRZEŻENIE
                status = StatusAudytu.OSTRZEZ if ostrz_okresu else StatusAudytu.OK
                self._wyniki.append(PunktKontroli(
                    konto=numer_ks,
                    punkt=f"Rachunek {opis}{bank_str}",
                    status=status,
                    uwagi=f"Saldo ZOiS zgodne z wyciągiem.{ostrz_okresu}{okres_info}",
                    wartosc=f"{saldo_zois:,.2f} zł",
                ))
            else:
                self._wyniki.append(PunktKontroli(
                    konto=numer_ks,
                    punkt=f"Rachunek {opis}{bank_str}",
                    status=StatusAudytu.BLAD,
                    uwagi=(
                        f"NIEZGODNOŚĆ: ZOiS={saldo_zois:,.2f} zł | "
                        f"Wyciąg={wyciag.saldo_koncowe:,.2f} zł | "
                        f"Różnica={roznica:,.2f} zł."
                        f"{ostrz_okresu}{okres_info}"
                    ),
                ))

        # Podsumowanie rachunków BEZ wyciągu
        if rachunki_bez_wyciagu:
            lista = "; ".join(
                f"{nr} ({op}): {s:,.2f} zł"
                for nr, op, s in rachunki_bez_wyciagu
            )
            self._wyniki.append(PunktKontroli(
                konto="130 (podsum.)",
                punkt=f"Rachunki bez wgranego wyciągu ({len(rachunki_bez_wyciagu)} szt.)",
                status=StatusAudytu.OSTRZEZ,
                uwagi=f"Do uzupełnienia: {lista}",
            ))

    # ─── Pozostałe reguły (jak w v1.0) ───────────────────────────────────────

    def _saldo(self, dane_zois: DaneZOiS, konto: str):
        return dane_zois.konta.get(konto)

    def _weryfikuj_konto_145(self, dane_zois: DaneZOiS):
        s = self._saldo(dane_zois, "145")
        if s is None:
            self._wyniki.append(PunktKontroli(
                konto="145", punkt="Środki pieniężne w drodze = 0",
                status=StatusAudytu.INFO, uwagi="Konto 145 nie wystąpiło w ZOiS.",
            )); return
        wn, ma = s
        if wn == Decimal("0") and ma == Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="145", punkt="Środki pieniężne w drodze = 0",
                status=StatusAudytu.OK, uwagi="Saldo wynosi 0.",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="145", punkt="Środki pieniężne w drodze = 0",
                status=StatusAudytu.BLAD,
                uwagi=f"Saldo ≠ 0! Wn={wn:,.2f}, Ma={ma:,.2f}.",
            ))

    def _weryfikuj_konto_200(self, dane_zois: DaneZOiS):
        s = self._saldo(dane_zois, "200")
        if s is None:
            self._wyniki.append(PunktKontroli(
                konto="200", punkt="Rozrachunki z odbiorcami",
                status=StatusAudytu.BRAK,
                uwagi="CRITICAL: Brak konta 200.",
            )); return
        wn, ma = s
        if ma > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="200", punkt="Rozrachunki z odbiorcami – strona salda",
                status=StatusAudytu.BLAD,
                uwagi=(f"Saldo Ma={ma:,.2f} zł – BŁĄD. "
                       "Brak faktury sprzedaży lub nadpłata."),
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="200", punkt="Rozrachunki z odbiorcami – strona salda",
                status=StatusAudytu.OK,
                uwagi=f"Saldo Wn={wn:,.2f} zł – należności.",
            ))

    def _weryfikuj_konto_202(self, dane_zois: DaneZOiS):
        s = self._saldo(dane_zois, "202")
        if s is None:
            self._wyniki.append(PunktKontroli(
                konto="202", punkt="Rozrachunki z dostawcami",
                status=StatusAudytu.BRAK, uwagi="Brak konta 202 w ZOiS.",
            )); return
        wn, ma = s
        if wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="202", punkt="Rozrachunki z dostawcami – strona salda",
                status=StatusAudytu.BLAD,
                uwagi=(f"Saldo Wn={wn:,.2f} zł – BŁĄD. "
                       "Nadpłata lub brak faktury zakupu."),
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="202", punkt="Rozrachunki z dostawcami – strona salda",
                status=StatusAudytu.OK,
                uwagi=f"Saldo Ma={ma:,.2f} zł – zobowiązania.",
            ))

    def _weryfikuj_konto_230(self, dane_zois: DaneZOiS):
        s = self._saldo(dane_zois, "230")
        if s is None:
            self._wyniki.append(PunktKontroli(
                konto="230", punkt="Wynagrodzenia",
                status=StatusAudytu.INFO, uwagi="Konto 230 nie wystąpiło.",
            )); return
        wn, ma = s
        if wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="230", punkt="Rozrachunki z pracownikami",
                status=StatusAudytu.OSTRZEZ,
                uwagi=f"Saldo Wn={wn:,.2f} zł – brak LP lub nadpłata.",
            ))
        elif ma > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="230", punkt="Rozrachunki z pracownikami",
                status=StatusAudytu.OK,
                uwagi=f"Saldo Ma={ma:,.2f} zł – niezapłacone (dopuszczalne).",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="230", punkt="Rozrachunki z pracownikami",
                status=StatusAudytu.OK, uwagi="Saldo 230 = 0.",
            ))

    def _weryfikuj_konto_229(self, dane_zois: DaneZOiS):
        s = self._saldo(dane_zois, "229")
        if s is None:
            self._wyniki.append(PunktKontroli(
                konto="229", punkt="ZUS",
                status=StatusAudytu.BRAK, uwagi="Brak konta 229.",
            )); return
        wn, ma = s
        if wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="229", punkt="ZUS – zobowiązanie",
                status=StatusAudytu.OSTRZEZ,
                uwagi=f"Saldo Wn={wn:,.2f} zł – nadpłata/błąd DRA.",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="229", punkt="ZUS – zobowiązanie",
                status=StatusAudytu.OK,
                uwagi=f"Saldo Ma={ma:,.2f} zł. Porównaj z DRA.",
            ))

    def _weryfikuj_konto_220(self, dane_zois: DaneZOiS):
        s = self._saldo(dane_zois, "220")
        if s is None:
            self._wyniki.append(PunktKontroli(
                konto="220", punkt="US/PIT",
                status=StatusAudytu.BRAK, uwagi="Brak konta 220.",
            )); return
        wn, ma = s
        if wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="220", punkt="US – PIT",
                status=StatusAudytu.OSTRZEZ,
                uwagi=f"Saldo Wn={wn:,.2f} zł – nadpłata/brak dekretacji XII.",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="220", punkt="US – PIT",
                status=StatusAudytu.OK,
                uwagi=f"Saldo Ma={ma:,.2f} zł. Porównaj z PIT-4R/8AR.",
            ))

    def _weryfikuj_konto_700(self, dane_zois: DaneZOiS):
        s = self._saldo(dane_zois, "700")
        if s is None:
            self._wyniki.append(PunktKontroli(
                konto="700", punkt="Przychody",
                status=StatusAudytu.BRAK,
                uwagi="CRITICAL: Brak konta 700.",
            )); return
        wn, ma = s
        if wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="700", punkt="Przychody – przychodowość",
                status=StatusAudytu.BLAD,
                uwagi=f"Saldo Wn={wn:,.2f} zł – BŁĄD! Konto wynikowe (Ma).",
            ))
        elif ma > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="700", punkt="Przychody – przychodowość",
                status=StatusAudytu.OK,
                uwagi=f"Saldo Ma={ma:,.2f} zł – poprawne.",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="700", punkt="Przychody – przychodowość",
                status=StatusAudytu.OSTRZEZ,
                uwagi="Zerowe obroty – brak sprzedaży?",
            ))

    def _weryfikuj_grupe_4(self, dane_zois: DaneZOiS):
        konta = {k: v for k, v in dane_zois.konta.items() if get_grupa(k) == 4}
        if not konta:
            self._wyniki.append(PunktKontroli(
                konto="Grupa 4", punkt="Koszty rodzajowe (400–499)",
                status=StatusAudytu.BRAK, uwagi="Brak kont grupy 4.",
            )); return

        bledne = []
        suma = Decimal("0")
        for k, (wn, ma) in konta.items():
            suma += wn
            if ma > Decimal("0"):
                opis = dane_zois.opisy.get(k, "")
                bledne.append(f"{k} ({opis}): Ma={ma:,.2f} zł")

        if bledne:
            self._wyniki.append(PunktKontroli(
                konto="Grupa 4", punkt="Koszty rodzajowe – tylko Saldo Wn",
                status=StatusAudytu.BLAD,
                uwagi=(f"Konta z Saldem Ma ({len(bledne)} szt.): "
                       + "; ".join(bledne[:5])
                       + (" ..." if len(bledne) > 5 else "")),
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="Grupa 4", punkt="Koszty rodzajowe – tylko Saldo Wn",
                status=StatusAudytu.OK,
                uwagi=(f"Wszystkie {len(konta)} kont poprawne. "
                       f"Suma kosztów: {suma:,.2f} zł."),
            ))

    def _weryfikuj_bilans(self, dane_bilans: Optional[DaneBilansu]):
        if dane_bilans is None:
            self._wyniki.append(PunktKontroli(
                konto="Bilans", punkt="Suma bilansowa",
                status=StatusAudytu.BRAK, uwagi="Nie dostarczono Bilansu.",
            )); return

        TOL = Decimal("0.01")
        rb = abs(dane_bilans.aktywa_biezacy - dane_bilans.pasywa_biezacy)
        if rb <= TOL:
            self._wyniki.append(PunktKontroli(
                konto="Bilans", punkt="Suma bilansowa rok bieżący",
                status=StatusAudytu.OK,
                uwagi=f"Aktywa = Pasywa = {dane_bilans.aktywa_biezacy:,.2f} zł.",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="Bilans", punkt="Suma bilansowa rok bieżący",
                status=StatusAudytu.BLAD,
                uwagi=(f"NIEZGODNOŚĆ! A={dane_bilans.aktywa_biezacy:,.2f} ≠ "
                       f"P={dane_bilans.pasywa_biezacy:,.2f} | Δ={rb:,.2f} zł."),
            ))

        if dane_bilans.aktywa_ubiegly > Decimal("0") or dane_bilans.pasywa_ubiegly > Decimal("0"):
            ru = abs(dane_bilans.aktywa_ubiegly - dane_bilans.pasywa_ubiegly)
            if ru <= TOL:
                self._wyniki.append(PunktKontroli(
                    konto="Bilans", punkt="Suma bilansowa rok ubiegły",
                    status=StatusAudytu.OK,
                    uwagi=f"Dane porównawcze: {dane_bilans.aktywa_ubiegly:,.2f} zł.",
                ))
            else:
                self._wyniki.append(PunktKontroli(
                    konto="Bilans", punkt="Suma bilansowa rok ubiegły",
                    status=StatusAudytu.BLAD,
                    uwagi=(f"NIEZGODNOŚĆ porównawczych! "
                           f"A={dane_bilans.aktywa_ubiegly:,.2f} ≠ "
                           f"P={dane_bilans.pasywa_ubiegly:,.2f} | Δ={ru:,.2f} zł."),
                ))


# =============================================================================
# TRYB TESTOWY
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("  SymfoniaYearEndAuditor v2.0 – Tryb Testowy")
    print("  Scenariusz: 3 rachunki bankowe, różne miesiące, 1 bez wyciągu")
    print("=" * 70)

    dane_zois = DaneZOiS()
    # Wiele rachunków – różne analityki konta 130
    dane_zois.konta_analityki = {
        "130-1": (Decimal("125000.00"), Decimal("0")),   # Santander, aktywny
        "130-2": (Decimal("3500.00"),   Decimal("0")),   # mBank, nieużywany
        "130-3": (Decimal("18000.00"),  Decimal("0")),   # PKO walutowy
    }
    dane_zois.konta = {
        "130":  (Decimal("146500.00"), Decimal("0")),
        "145":  (Decimal("0"),         Decimal("0")),
        "200":  (Decimal("12000.00"),  Decimal("0")),
        "202":  (Decimal("0"),         Decimal("8750.00")),
        "400":  (Decimal("45000.00"),  Decimal("0")),
        "700":  (Decimal("0"),         Decimal("210000.00")),
    }
    dane_zois.opisy = {
        "130-1": "Santander – rachunek główny",
        "130-2": "mBank – rachunek pomocniczy",
        "130-3": "PKO BP – rachunek walutowy USD",
    }

    # Wyciągi – tylko dla 2 z 3 rachunków
    wyciagi = [
        WyciagBankowy(
            numer_konta_ksiegowego="130-1",
            saldo_koncowe=Decimal("125000.00"),
            rok_ostatniej_operacji=2024,
            miesiac_ostatniej_operacji=12,  # grudzień – OK
            bank_nazwa="Santander",
        ),
        WyciagBankowy(
            numer_konta_ksiegowego="130-2",
            saldo_koncowe=Decimal("3500.00"),
            rok_ostatniej_operacji=2024,
            miesiac_ostatniej_operacji=4,   # kwiecień – OSTRZEŻENIE
            bank_nazwa="mBank",
        ),
        # BRAK wyciągu dla 130-3 → powinien być listed osobno
    ]

    dane_bilans = DaneBilansu(
        aktywa_biezacy=Decimal("350000.00"),
        pasywa_biezacy=Decimal("350000.00"),
    )

    audytor = SymfoniaYearEndAuditor()
    wyniki = audytor.check_accounting_logic(
        dane_zois, dane_bilans, wyciagi, rok_obrachunkowy=2024,
    )
    raport = audytor.generate_audit_report(wyniki, "TESTOWA SP. Z O.O.", 2024)
    print(raport["tekst"])
