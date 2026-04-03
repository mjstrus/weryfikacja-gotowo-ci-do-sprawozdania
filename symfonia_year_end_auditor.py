"""
=============================================================================
SymfoniaYearEndAuditor – Automatyczna Kontrola Jakości Danych Finansowych
Biuro Rachunkowe Abacus | Zamknięcie Roku Obrachunkowego
=============================================================================
Moduł weryfikuje spójność danych między:
  - ZOiS (Zestawienie Obrotów i Sald) – źródło główne
  - Bilansem (Aktywa = Pasywa)
  - Wyciągiem Bankowym (konto 130)

Kompatybilny z Frappe Framework (Server Script / Python Background Job).
Przetwarza dane wyłącznie w pamięci (in-memory) – brak zapisu plików tymczasowych.

Obsługiwane formaty wejściowe:
  - Symfonia Finanse i Księgowość → eksport XLSX / PDF
=============================================================================
"""

from __future__ import annotations

import io
import re
import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

# ── Opcjonalne biblioteki – importowane leniwie ───────────────────────────────
try:
    import pdfplumber  # pip install pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

try:
    import openpyxl  # pip install openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

# ── Konfiguracja logowania ────────────────────────────────────────────────────
logger = logging.getLogger("SymfoniaAuditor")
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")


# =============================================================================
# TYPY POMOCNICZE
# =============================================================================

class StatusAudytu(Enum):
    """Status pojedynczego punktu kontrolnego w raporcie."""
    OK       = "✅ OK"
    BLAD     = "❌ BŁĄD"
    OSTRZEZ  = "⚠️  OSTRZEŻENIE"
    BRAK     = "🔍 BRAK DANYCH"
    INFO     = "ℹ️  INFO"


@dataclass
class PunktKontroli:
    """Pojedynczy wynik weryfikacji – wiersz raportu dla księgowego."""
    konto:   str           # np. "130", "Bilans", "Grupa 4"
    punkt:   str           # opis reguły
    status:  StatusAudytu
    uwagi:   str = ""
    wartosc: str = ""      # opcjonalna wartość liczbowa do wyświetlenia


@dataclass
class DaneZOiS:
    """
    Zestawienie Obrotów i Sald sparsowane z Symfonii.
    Słownik: numer_konta_syntetycznego → (saldo_wn, saldo_ma)
    Kwoty jako Decimal dla precyzji księgowej.
    """
    konta: Dict[str, Tuple[Decimal, Decimal]] = field(default_factory=dict)
    # Mapowanie konto → opis (nazwa konta z ZOiS)
    opisy: Dict[str, str] = field(default_factory=dict)


@dataclass
class DaneBilansu:
    """Dane bilansu: sumy aktywów i pasywów za rok bieżący i ubiegły."""
    aktywa_biezacy:  Decimal = Decimal("0")
    pasywa_biezacy:  Decimal = Decimal("0")
    aktywa_ubiegly:  Decimal = Decimal("0")
    pasywa_ubiegly:  Decimal = Decimal("0")


@dataclass
class DaneWyciagu:
    """Dane wyciągu bankowego: saldo końcowe rachunku (grudzień)."""
    saldo_koncowe: Decimal = Decimal("0")
    bank_nazwa:    str = ""


# =============================================================================
# FUNKCJE NORMALIZACJI DANYCH
# =============================================================================

def normalize_currency(wartosc: Union[str, float, int, None]) -> Decimal:
    """
    Normalizuje kwotę finansową do obiektu Decimal.

    Symfonia może generować kwoty w różnych formatach:
      - "1 234,56"  (spacja jako separator tysięcy, przecinek dziesiętny – PL)
      - "1.234,56"  (kropka jako separator tysięcy, przecinek dziesiętny)
      - "1,234.56"  (angielski format)
      - "1234.56"   (bez separatora tysięcy)
      - "-1 234,56" (wartości ujemne)
      - ""          (puste pole = 0)
      - None        (brak wartości = 0)

    Zwraca:
        Decimal z zaokrągleniem do 2 miejsc po przecinku.

    Rzuca:
        ValueError jeśli wartość nie może być zinterpretowana jako kwota.
    """
    if wartosc is None:
        return Decimal("0")

    # Konwersja z float/int – typowe przy odczycie z Excel przez pandas
    if isinstance(wartosc, (int, float)):
        try:
            return Decimal(str(round(float(wartosc), 2)))
        except (InvalidOperation, ValueError):
            return Decimal("0")

    tekst = str(wartosc).strip()

    if not tekst or tekst in ("-", "–", "0,00", "0.00"):
        return Decimal("0")

    # Usunięcie znaku waluty i niechcianych znaków (zł, PLN, nbsp)
    tekst = re.sub(r"[złZŁPLN\xa0]", "", tekst).strip()

    # Wykrycie formatu separatorów
    # Format PL: "1 234,56" lub "1.234,56"
    if "," in tekst and "." in tekst:
        if tekst.rfind(",") > tekst.rfind("."):
            # PL format: 1.234,56 → ostatni przecinek = dziesiętny
            tekst = tekst.replace(".", "").replace(",", ".")
        else:
            # EN format: 1,234.56 → ostatnia kropka = dziesiętny
            tekst = tekst.replace(",", "")
    elif "," in tekst:
        # Tylko przecinek – może być dziesiętny (PL) lub separator tysięcy
        # Jeśli po przecinku są dokładnie 2 cyfry → dziesiętny
        czesci = tekst.split(",")
        if len(czesci) == 2 and len(czesci[1]) <= 2:
            tekst = tekst.replace(",", ".")
        else:
            # Separator tysięcy
            tekst = tekst.replace(",", "")

    # Usunięcie spacji (separator tysięcy w PL)
    tekst = tekst.replace(" ", "")

    try:
        wynik = Decimal(tekst)
        return wynik.quantize(Decimal("0.01"))
    except InvalidOperation:
        raise ValueError(
            f"Nie można znormalizować kwoty: '{wartosc}' → '{tekst}'. "
            "Sprawdź format eksportu Symfonii."
        )


def normalize_konto(numer: str) -> str:
    """
    Normalizuje numer konta księgowego do poziomu syntetyki.

    Symfonia generuje konta analityczne w formacie:
        201-1-001, 400-01, 130-2, itd.
    Funkcja zwraca segment główny (przed pierwszym myślnikiem).

    Przykłady:
        "201-1-001" → "201"
        "400-01"    → "400"
        "130"       → "130"
        "4"         → "4"    (konta syntetyczne jednocyfrowe = suma grupy)
    """
    if not numer:
        return ""
    return str(numer).strip().split("-")[0].strip()


def get_grupa(numer_konta: str) -> Optional[int]:
    """
    Zwraca numer grupy konta (pierwsza cyfra).
    Np. "401" → 4, "130" → 1, "700" → 7.
    """
    syntetyka = normalize_konto(numer_konta)
    if syntetyka and syntetyka[0].isdigit():
        return int(syntetyka[0])
    return None


# =============================================================================
# PARSERY DANYCH
# =============================================================================

class ParserZOiS:
    """
    Parser Zestawienia Obrotów i Sald eksportowanego z Symfonii.

    Symfonia generuje ZOiS z kolumnami (nazwy mogą się różnić między wersjami):
      - Konto / Numer konta
      - Nazwa konta
      - Saldo Wn (debet) – saldo po stronie Wn
      - Saldo Ma (kredyt) – saldo po stronie Ma

    Parser agreguje konta analityczne do syntetyki.
    """

    # Możliwe nazwy kolumn w eksportach Symfonii (różne wersje)
    KOLUMNY_KONTO = ["konto", "numer konta", "nr konta", "symbol konta", "account"]
    KOLUMNY_NAZWA = ["nazwa", "nazwa konta", "opis", "name"]
    KOLUMNY_SALDO_WN = [
        "saldo wn", "saldo_wn", "debet", "wn", "saldo dt",
        "saldo debetowe", "dt", "należności"
    ]
    KOLUMNY_SALDO_MA = [
        "saldo ma", "saldo_ma", "kredyt", "ma", "saldo ct",
        "saldo kredytowe", "ct", "zobowiązania"
    ]

    def _znajdz_kolumne(
        self, df: pd.DataFrame, kandydaci: List[str]
    ) -> Optional[str]:
        """Wyszukuje kolumnę po znormalizowanej nazwie (case-insensitive)."""
        kolumny_lower = {k.lower().strip(): k for k in df.columns}
        for kandydat in kandydaci:
            if kandydat.lower() in kolumny_lower:
                return kolumny_lower[kandydat.lower()]
        return None

    def parsuj_xlsx(self, dane_binarne: bytes) -> DaneZOiS:
        """
        Parsuje ZOiS z pliku XLSX wygenerowanego przez Symfonię.
        Obsługuje scalone komórki i nagłówki wielowierszowe.

        Args:
            dane_binarne: Zawartość pliku .xlsx jako bytes (in-memory).

        Returns:
            DaneZOiS ze słownikiem kont i saldami.
        """
        bufor = io.BytesIO(dane_binarne)

        # Wczytanie arkusza z openpyxl – obsługa scalonych komórek
        try:
            wb = openpyxl.load_workbook(bufor, data_only=True)
        except Exception as e:
            raise ValueError(f"Błąd odczytu XLSX (ZOiS): {e}")

        # Znalezienie arkusza ZOiS – Symfonia może używać różnych nazw
        nazwy_arkuszy = wb.sheetnames
        arkusz = None
        for nazwa in ["ZOiS", "Zestawienie", "ObrotyiSalda", "Sheet1", nazwy_arkuszy[0]]:
            if nazwa in nazwy_arkuszy:
                arkusz = wb[nazwa]
                break

        if arkusz is None:
            arkusz = wb.active

        logger.info(f"Parsowanie ZOiS – arkusz: '{arkusz.title}'")

        # Rozwiązanie scalonych komórek (unmerge + wypełnienie wartością)
        # Uwaga: openpyxl przy data_only=True nie zachowuje scalenia jako problemu,
        # ale wartości z scalonych komórek mogą być w pierwszej komórce zakresu.
        wiersze = []
        for wiersz in arkusz.iter_rows(values_only=True):
            wiersze.append(list(wiersz))

        if not wiersze:
            raise ValueError("Arkusz ZOiS jest pusty.")

        # Znalezienie wiersza nagłówkowego
        # Symfonia może mieć kilka wierszy tytułowych przed nagłówkiem kolumn
        idx_naglowka = self._znajdz_wiersz_naglowka(wiersze)
        if idx_naglowka is None:
            raise ValueError(
                "Nie znaleziono wiersza nagłówkowego w ZOiS. "
                "Sprawdź czy eksport zawiera kolumny: Konto, Saldo Wn, Saldo Ma."
            )

        naglowki = [str(k).strip() if k is not None else "" for k in wiersze[idx_naglowka]]
        dane_wiersze = wiersze[idx_naglowka + 1:]

        df = pd.DataFrame(dane_wiersze, columns=naglowki)
        return self._parsuj_dataframe(df, zrodlo="XLSX")

    def _znajdz_wiersz_naglowka(self, wiersze: list) -> Optional[int]:
        """Heurystycznie wykrywa wiersz nagłówkowy ZOiS."""
        slowa_kluczowe = {"konto", "saldo", "wn", "ma", "obroty", "numer"}
        for i, wiersz in enumerate(wiersze[:20]):  # Szukaj w pierwszych 20 wierszach
            wartosci = {str(v).lower().strip() for v in wiersz if v is not None}
            # Wiersz nagłówkowy zawiera słowa kluczowe
            if len(wartosci & slowa_kluczowe) >= 2:
                return i
        return None

    def parsuj_pdf(self, dane_binarne: bytes) -> DaneZOiS:
        """
        Parsuje ZOiS z pliku PDF wygenerowanego przez Symfonię.
        Używa biblioteki pdfplumber do ekstrakcji tabel.

        Args:
            dane_binarne: Zawartość pliku .pdf jako bytes (in-memory).

        Returns:
            DaneZOiS ze słownikiem kont i saldami.
        """
        if not PDF_AVAILABLE:
            raise ImportError(
                "Biblioteka 'pdfplumber' nie jest zainstalowana. "
                "Uruchom: pip install pdfplumber"
            )

        bufor = io.BytesIO(dane_binarne)
        wszystkie_wiersze = []
        naglowki = None

        try:
            with pdfplumber.open(bufor) as pdf:
                logger.info(f"Parsowanie ZOiS PDF – {len(pdf.pages)} stron.")

                for nr_strony, strona in enumerate(pdf.pages, 1):
                    # Ekstrakcja tabeli ze strony
                    tabele = strona.extract_tables(
                        table_settings={
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                            "snap_tolerance": 3,
                        }
                    )

                    for tabela in tabele:
                        if not tabela:
                            continue

                        # Na pierwszej stronie szukamy nagłówka
                        for idx_w, wiersz in enumerate(tabela):
                            if wiersz is None:
                                continue
                            wartosci = [str(v or "").strip() for v in wiersz]

                            if naglowki is None:
                                # Sprawdź czy to wiersz nagłówkowy
                                wartosci_lower = {v.lower() for v in wartosci}
                                slowa = {"konto", "saldo", "wn", "ma"}
                                if len(wartosci_lower & slowa) >= 2:
                                    naglowki = wartosci
                                    continue

                            if naglowki is not None and any(wartosci):
                                wszystkie_wiersze.append(wartosci)

        except Exception as e:
            raise ValueError(f"Błąd parsowania PDF (ZOiS): {e}")

        if naglowki is None or not wszystkie_wiersze:
            raise ValueError(
                "Nie znaleziono danych tabelarycznych w PDF (ZOiS). "
                "Upewnij się, że PDF zawiera tabelę z nagłówkami Konto/Saldo Wn/Saldo Ma."
            )

        df = pd.DataFrame(wszystkie_wiersze, columns=naglowki)
        return self._parsuj_dataframe(df, zrodlo="PDF")

    def _parsuj_dataframe(self, df: pd.DataFrame, zrodlo: str) -> DaneZOiS:
        """
        Przetwarza DataFrame na strukturę DaneZOiS.
        Agreguje konta analityczne do poziomu syntetyki.
        """
        # Znalezienie kolumn
        kol_konto = self._znajdz_kolumne(df, self.KOLUMNY_KONTO)
        kol_nazwa = self._znajdz_kolumne(df, self.KOLUMNY_NAZWA)
        kol_wn    = self._znajdz_kolumne(df, self.KOLUMNY_SALDO_WN)
        kol_ma    = self._znajdz_kolumne(df, self.KOLUMNY_SALDO_MA)

        brakujace = []
        if not kol_konto: brakujace.append("Konto")
        if not kol_wn:    brakujace.append("Saldo Wn")
        if not kol_ma:    brakujace.append("Saldo Ma")

        if brakujace:
            dostepne = list(df.columns)
            raise ValueError(
                f"Brak wymaganych kolumn w ZOiS ({zrodlo}): {', '.join(brakujace)}. "
                f"Dostępne kolumny: {dostepne}"
            )

        wynik = DaneZOiS()

        for _, wiersz in df.iterrows():
            numer_raw = wiersz.get(kol_konto)
            if pd.isna(numer_raw) or not str(numer_raw).strip():
                continue

            numer_syntetyczny = normalize_konto(str(numer_raw))

            # Pomijanie wierszy podsumowujących (np. "Razem", "SUMA")
            if numer_syntetyczny.lower() in {"razem", "suma", "ogółem", "total"}:
                continue

            # Weryfikacja czy to poprawny numer konta (cyfry)
            if not re.match(r"^\d+", numer_syntetyczny):
                continue

            try:
                saldo_wn = normalize_currency(wiersz.get(kol_wn))
                saldo_ma = normalize_currency(wiersz.get(kol_ma))
            except ValueError as e:
                logger.warning(f"Pominięto konto {numer_syntetyczny}: {e}")
                continue

            # Agregacja do syntetyki (sumowanie kont analitycznych)
            istniejace_wn, istniejace_ma = wynik.konta.get(
                numer_syntetyczny, (Decimal("0"), Decimal("0"))
            )
            wynik.konta[numer_syntetyczny] = (
                istniejace_wn + saldo_wn,
                istniejace_ma + saldo_ma,
            )

            # Zapis opisu konta
            if kol_nazwa and numer_syntetyczny not in wynik.opisy:
                opis_raw = wiersz.get(kol_nazwa)
                if not pd.isna(opis_raw):
                    wynik.opisy[numer_syntetyczny] = str(opis_raw).strip()

        logger.info(
            f"ZOiS ({zrodlo}): sparsowano {len(wynik.konta)} kont syntetycznych."
        )
        return wynik


class ParserBilansu:
    """
    Parser Bilansu eksportowanego z Symfonii.
    Szuka sum Aktywów i Pasywów dla roku bieżącego i ubiegłego.
    """

    KLUCZE_AKTYWA  = ["suma aktywów", "aktywa razem", "total assets", "a k t y w a"]
    KLUCZE_PASYWA  = ["suma pasywów", "pasywa razem", "total liabilities", "p a s y w a"]

    def parsuj_xlsx(self, dane_binarne: bytes) -> DaneBilansu:
        """Parsuje Bilans z XLSX (eksport Symfonii)."""
        bufor = io.BytesIO(dane_binarne)
        try:
            # Wczytaj z openpyxl żeby obsłużyć scalone komórki nagłówkowe
            wb = openpyxl.load_workbook(bufor, data_only=True)
            arkusz = wb.active
            wiersze = list(arkusz.iter_rows(values_only=True))
        except Exception as e:
            raise ValueError(f"Błąd odczytu XLSX (Bilans): {e}")

        return self._szukaj_sum_bilansowych(wiersze)

    def parsuj_pdf(self, dane_binarne: bytes) -> DaneBilansu:
        """Parsuje Bilans z PDF."""
        if not PDF_AVAILABLE:
            raise ImportError("Wymagana biblioteka 'pdfplumber'.")

        bufor = io.BytesIO(dane_binarne)
        wszystkie_wiersze = []

        with pdfplumber.open(bufor) as pdf:
            for strona in pdf.pages:
                tekst = strona.extract_text() or ""
                wszystkie_wiersze.extend(tekst.splitlines())

        # Konwersja do list (uproszczone dla PDF tekstowego)
        wiersze_jako_listy = [[w] for w in wszystkie_wiersze]
        return self._szukaj_sum_bilansowych(wiersze_jako_listy)

    def _szukaj_sum_bilansowych(self, wiersze: list) -> DaneBilansu:
        """
        Heurystycznie wyszukuje wiersze z sumami bilansowymi.
        Symfonia typowo ma format: [Opis] [Kwota bieżący rok] [Kwota rok ubiegły]
        """
        wynik = DaneBilansu()
        znaleziono_aktywa  = False
        znaleziono_pasywa  = False

        for wiersz in wiersze:
            if not wiersz:
                continue
            # Spłaszczenie wiersza do tekstu
            tekst_wiersza = " ".join(
                str(k).lower().strip() for k in wiersz if k is not None
            )

            # Wykrycie wiersza sumy aktywów
            if any(klucz in tekst_wiersza for klucz in self.KLUCZE_AKTYWA):
                kwoty = self._wyciagnij_kwoty_z_wiersza(wiersz)
                if kwoty:
                    wynik.aktywa_biezacy = kwoty[0]
                    wynik.aktywa_ubiegly = kwoty[1] if len(kwoty) > 1 else Decimal("0")
                    znaleziono_aktywa = True
                    logger.info(
                        f"Bilans – Suma Aktywów: bieżący={wynik.aktywa_biezacy}, "
                        f"ubiegły={wynik.aktywa_ubiegly}"
                    )

            # Wykrycie wiersza sumy pasywów
            elif any(klucz in tekst_wiersza for klucz in self.KLUCZE_PASYWA):
                kwoty = self._wyciagnij_kwoty_z_wiersza(wiersz)
                if kwoty:
                    wynik.pasywa_biezacy = kwoty[0]
                    wynik.pasywa_ubiegly = kwoty[1] if len(kwoty) > 1 else Decimal("0")
                    znaleziono_pasywa = True
                    logger.info(
                        f"Bilans – Suma Pasywów: bieżący={wynik.pasywa_biezacy}, "
                        f"ubiegły={wynik.pasywa_ubiegly}"
                    )

        if not znaleziono_aktywa:
            logger.warning("Nie znaleziono sumy Aktywów w Bilansie.")
        if not znaleziono_pasywa:
            logger.warning("Nie znaleziono sumy Pasywów w Bilansie.")

        return wynik

    def _wyciagnij_kwoty_z_wiersza(self, wiersz: list) -> List[Decimal]:
        """Wyciąga wszystkie kwoty liczbowe z wiersza bilansowego."""
        kwoty = []
        for komorka in wiersz:
            if komorka is None:
                continue
            try:
                kwota = normalize_currency(komorka)
                if kwota != Decimal("0"):
                    kwoty.append(kwota)
            except ValueError:
                continue
        return kwoty


# =============================================================================
# GŁÓWNA KLASA AUDYTORA
# =============================================================================

class SymfoniaYearEndAuditor:
    """
    Audytor jakości danych finansowych – zamknięcie roku obrachunkowego.

    Kompatybilny z Frappe Framework jako Server Script.

    Przykład użycia w Frappe (Server Script):
    ─────────────────────────────────────────
        from symfonia_year_end_auditor import SymfoniaYearEndAuditor

        audytor = SymfoniaYearEndAuditor()

        # Wczytanie plików z załączników Frappe (in-memory)
        zois_bytes  = frappe.get_doc("File", zois_file_id).get_content()
        bil_bytes   = frappe.get_doc("File", bil_file_id).get_content()
        bank_bytes  = frappe.get_doc("File", bank_file_id).get_content()

        raport = audytor.run_full_audit(
            zois_bytes=zois_bytes,   zois_format="xlsx",
            bilans_bytes=bil_bytes,  bilans_format="xlsx",
            bank_bytes=bank_bytes,   bank_format="xlsx",
        )
        frappe.msgprint(raport["tekst"])
    ─────────────────────────────────────────
    """

    def __init__(self):
        self._parser_zois   = ParserZOiS()
        self._parser_bilans = ParserBilansu()
        self._wyniki: List[PunktKontroli] = []

    # ─── PUBLICZNE API ────────────────────────────────────────────────────────

    def load_data(
        self,
        *,
        zois_bytes:   Optional[bytes] = None,
        zois_format:  str = "xlsx",
        bilans_bytes: Optional[bytes] = None,
        bilans_format: str = "xlsx",
        bank_bytes:   Optional[bytes] = None,
        bank_format:  str = "xlsx",
        saldo_bankowe_reczne: Optional[Decimal] = None,
    ) -> Tuple[Optional[DaneZOiS], Optional[DaneBilansu], Optional[Decimal]]:
        """
        Wczytuje i parsuje dane wejściowe z plików Symfonii.

        Args:
            zois_bytes:    Plik ZOiS jako bytes.
            zois_format:   Format: "xlsx" lub "pdf".
            bilans_bytes:  Plik Bilansu jako bytes.
            bilans_format: Format: "xlsx" lub "pdf".
            bank_bytes:    Wyciąg bankowy jako bytes.
            bank_format:   Format: "xlsx" lub "pdf".
            saldo_bankowe_reczne: Alternatywnie – podaj saldo bankowe ręcznie (Decimal).

        Returns:
            Krotka (DaneZOiS, DaneBilansu, saldo_bankowe).
        """
        dane_zois   = None
        dane_bilans = None
        saldo_bank  = saldo_bankowe_reczne

        # ── Parsowanie ZOiS ──────────────────────────────────────────────────
        if zois_bytes:
            logger.info("Wczytywanie ZOiS...")
            try:
                if zois_format.lower() == "xlsx":
                    dane_zois = self._parser_zois.parsuj_xlsx(zois_bytes)
                elif zois_format.lower() == "pdf":
                    dane_zois = self._parser_zois.parsuj_pdf(zois_bytes)
                else:
                    raise ValueError(f"Nieobsługiwany format ZOiS: {zois_format}")
            except Exception as e:
                logger.error(f"Błąd parsowania ZOiS: {e}")
                raise

        # ── Parsowanie Bilansu ───────────────────────────────────────────────
        if bilans_bytes:
            logger.info("Wczytywanie Bilansu...")
            try:
                if bilans_format.lower() == "xlsx":
                    dane_bilans = self._parser_bilans.parsuj_xlsx(bilans_bytes)
                elif bilans_format.lower() == "pdf":
                    dane_bilans = self._parser_bilans.parsuj_pdf(bilans_bytes)
                else:
                    raise ValueError(f"Nieobsługiwany format Bilansu: {bilans_format}")
            except Exception as e:
                logger.error(f"Błąd parsowania Bilansu: {e}")
                raise

        # ── Parsowanie Wyciągu Bankowego ─────────────────────────────────────
        if bank_bytes and saldo_bank is None:
            logger.info("Wczytywanie Wyciągu Bankowego...")
            try:
                saldo_bank = self._parsuj_wyciag_bankowy(bank_bytes, bank_format)
            except Exception as e:
                logger.warning(f"Błąd parsowania wyciągu bankowego: {e}")
                # Nie przerywamy – saldo może być podane ręcznie

        return dane_zois, dane_bilans, saldo_bank

    def check_accounting_logic(
        self,
        dane_zois:   Optional[DaneZOiS],
        dane_bilans: Optional[DaneBilansu],
        saldo_bank:  Optional[Decimal],
    ) -> List[PunktKontroli]:
        """
        Główna funkcja weryfikacji logiki księgowej.

        Realizuje wszystkie reguły z tabeli wymagań:
          - Konto 130: zgodność z wyciągiem bankowym
          - Konto 145: saldo musi = 0
          - Konto 200: alert przy saldzie Ma
          - Konto 202: alert przy saldzie Wn
          - Konto 230: informacja o saldach
          - Konto 229: zobowiązanie ZUS
          - Konto 220: zobowiązanie PIT
          - Konto 700: weryfikacja przychodowości
          - Grupa 4: tylko saldo Wn
          - Bilans: Aktywa = Pasywa

        Returns:
            Lista punktów kontroli z wynikami.
        """
        self._wyniki = []

        if dane_zois is None:
            self._wyniki.append(PunktKontroli(
                konto="ZOiS",
                punkt="Wczytanie Zestawienia Obrotów i Sald",
                status=StatusAudytu.BRAK,
                uwagi="Nie dostarczono pliku ZOiS. Weryfikacja kont niemożliwa.",
            ))
        else:
            # ── Weryfikacja poszczególnych kont ──────────────────────────────
            self._weryfikuj_konto_130(dane_zois, saldo_bank)
            self._weryfikuj_konto_145(dane_zois)
            self._weryfikuj_konto_200(dane_zois)
            self._weryfikuj_konto_202(dane_zois)
            self._weryfikuj_konto_230(dane_zois)
            self._weryfikuj_konto_229(dane_zois)
            self._weryfikuj_konto_220(dane_zois)
            self._weryfikuj_konto_700(dane_zois)
            self._weryfikuj_grupe_4(dane_zois)

        # ── Weryfikacja Bilansu ───────────────────────────────────────────────
        self._weryfikuj_bilans(dane_bilans)

        return self._wyniki

    def generate_audit_report(
        self,
        wyniki: List[PunktKontroli],
        nazwa_podmiotu: str = "Podmiot",
        rok: int = 2024,
    ) -> Dict:
        """
        Generuje czytelny raport dla księgowego.

        Returns:
            Słownik zawierający:
              - "tekst":  Sformatowany raport tekstowy (str)
              - "wyniki": Lista słowników z wynikami (dla Frappe ListView)
              - "podsumowanie": Słownik ze statystykami
        """
        linia = "═" * 70

        # ── Nagłówek raportu ─────────────────────────────────────────────────
        linie_raportu = [
            linia,
            f"  RAPORT KONTROLI JAKOŚCI DANYCH – ZAMKNIĘCIE ROKU {rok}",
            f"  Podmiot: {nazwa_podmiotu}",
            f"  Wygenerowano przez: SymfoniaYearEndAuditor v1.0",
            linia,
            "",
        ]

        # ── Tabela wyników ────────────────────────────────────────────────────
        linie_raportu.append(
            f"{'KONTO':<10} {'STATUS':<20} {'PUNKT KONTROLI':<40} UWAGI"
        )
        linie_raportu.append("─" * 100)

        statystyki = {
            StatusAudytu.OK:      0,
            StatusAudytu.BLAD:    0,
            StatusAudytu.OSTRZEZ: 0,
            StatusAudytu.BRAK:    0,
            StatusAudytu.INFO:    0,
        }

        wyniki_slowniki = []

        for pkt in wyniki:
            statystyki[pkt.status] += 1

            # Formatowanie wiersza tabeli
            wartosc_str = f" [{pkt.wartosc}]" if pkt.wartosc else ""
            uwagi_full = f"{pkt.uwagi}{wartosc_str}".strip()

            linie_raportu.append(
                f"{pkt.konto:<10} {pkt.status.value:<20} "
                f"{pkt.punkt:<40} {uwagi_full}"
            )

            wyniki_slowniki.append({
                "konto":   pkt.konto,
                "status":  pkt.status.value,
                "punkt":   pkt.punkt,
                "uwagi":   pkt.uwagi,
                "wartosc": pkt.wartosc,
            })

        # ── Sekcja błędów krytycznych ─────────────────────────────────────────
        bledy_krytyczne = [p for p in wyniki if p.status == StatusAudytu.BLAD]
        ostrzezenia     = [p for p in wyniki if p.status == StatusAudytu.OSTRZEZ]

        linie_raportu.append("")
        linie_raportu.append(linia)
        linie_raportu.append("  PODSUMOWANIE")
        linie_raportu.append(linia)
        linie_raportu.append(f"  ✅ OK:            {statystyki[StatusAudytu.OK]}")
        linie_raportu.append(f"  ❌ BŁĘDY:         {statystyki[StatusAudytu.BLAD]}")
        linie_raportu.append(f"  ⚠️  OSTRZEŻENIA:  {statystyki[StatusAudytu.OSTRZEZ]}")
        linie_raportu.append(f"  🔍 BRAK DANYCH:  {statystyki[StatusAudytu.BRAK]}")

        if bledy_krytyczne:
            linie_raportu.append("")
            linie_raportu.append("  ❌ WYMAGANE DZIAŁANIA (BŁĘDY KRYTYCZNE):")
            for pkt in bledy_krytyczne:
                linie_raportu.append(f"    → Konto {pkt.konto}: {pkt.uwagi}")

        if ostrzezenia:
            linie_raportu.append("")
            linie_raportu.append("  ⚠️  DO WYJAŚNIENIA (OSTRZEŻENIA):")
            for pkt in ostrzezenia:
                linie_raportu.append(f"    → Konto {pkt.konto}: {pkt.uwagi}")

        # Ogólna ocena
        linie_raportu.append("")
        if statystyki[StatusAudytu.BLAD] == 0 and statystyki[StatusAudytu.OSTRZEZ] == 0:
            linie_raportu.append(
                "  🎉 OCENA KOŃCOWA: DANE SPÓJNE – gotowe do badania sprawozdania."
            )
        elif statystyki[StatusAudytu.BLAD] == 0:
            linie_raportu.append(
                "  🟡 OCENA KOŃCOWA: WYMAGA WYJAŚNIENIA – "
                "ostrzeżenia do weryfikacji z klientem."
            )
        else:
            linie_raportu.append(
                "  🔴 OCENA KOŃCOWA: DANE NIESPÓJNE – "
                "wymagane korekty przed zamknięciem roku."
            )

        linie_raportu.append(linia)

        tekst_raportu = "\n".join(linie_raportu)

        return {
            "tekst":       tekst_raportu,
            "wyniki":      wyniki_slowniki,
            "podsumowanie": {
                "ok":          statystyki[StatusAudytu.OK],
                "bledy":       statystyki[StatusAudytu.BLAD],
                "ostrzezenia": statystyki[StatusAudytu.OSTRZEZ],
                "brak_danych": statystyki[StatusAudytu.BRAK],
                "podmiot":     nazwa_podmiotu,
                "rok":         rok,
            }
        }

    def run_full_audit(
        self,
        *,
        zois_bytes:   Optional[bytes] = None,
        zois_format:  str = "xlsx",
        bilans_bytes: Optional[bytes] = None,
        bilans_format: str = "xlsx",
        bank_bytes:   Optional[bytes] = None,
        bank_format:  str = "xlsx",
        saldo_bankowe_reczne: Optional[Decimal] = None,
        nazwa_podmiotu: str = "Podmiot",
        rok: int = 2024,
    ) -> Dict:
        """
        Główna metoda wejściowa – uruchamia pełny audyt jednym wywołaniem.
        Łączy: load_data() → check_accounting_logic() → generate_audit_report()
        """
        dane_zois, dane_bilans, saldo_bank = self.load_data(
            zois_bytes=zois_bytes,         zois_format=zois_format,
            bilans_bytes=bilans_bytes,     bilans_format=bilans_format,
            bank_bytes=bank_bytes,         bank_format=bank_format,
            saldo_bankowe_reczne=saldo_bankowe_reczne,
        )

        wyniki = self.check_accounting_logic(dane_zois, dane_bilans, saldo_bank)

        return self.generate_audit_report(wyniki, nazwa_podmiotu=nazwa_podmiotu, rok=rok)

    # ─── PRYWATNE METODY WERYFIKACJI KONT ────────────────────────────────────

    def _pobierz_saldo(
        self, dane_zois: DaneZOiS, numer_konta: str
    ) -> Optional[Tuple[Decimal, Decimal]]:
        """
        Zwraca (saldo_wn, saldo_ma) dla konta syntetycznego.
        Szuka zarówno pełnego numeru jak i syntetyki.
        """
        # Próba bezpośredniego dopasowania
        if numer_konta in dane_zois.konta:
            return dane_zois.konta[numer_konta]

        # Agregacja wszystkich analityk pod daną syntetyką
        # (na wypadek gdyby nie były zagregowane podczas parsowania)
        pasujace = [
            (wn, ma)
            for konto, (wn, ma) in dane_zois.konta.items()
            if normalize_konto(konto) == numer_konta
        ]

        if pasujace:
            total_wn = sum((x[0] for x in pasujace), Decimal("0"))
            total_ma = sum((x[1] for x in pasujace), Decimal("0"))
            return total_wn, total_ma

        return None

    def _weryfikuj_konto_130(
        self, dane_zois: DaneZOiS, saldo_bank: Optional[Decimal]
    ) -> None:
        """
        Konto 130 – Rachunek bankowy bieżący.
        Reguła: Saldo ZOiS (konto 130) MUSI być równe saldu końcowemu
                wyciągu bankowego za grudzień.
        """
        saldo = self._pobierz_saldo(dane_zois, "130")

        if saldo is None:
            self._wyniki.append(PunktKontroli(
                konto="130",
                punkt="Rachunek bankowy vs wyciąg",
                status=StatusAudytu.BRAK,
                uwagi="CRITICAL: Nie odnaleziono konta 130 w ZOiS.",
            ))
            return

        saldo_wn, saldo_ma = saldo
        # Saldo konta 130 – aktywne, powinno być po stronie Wn
        saldo_ksiegowy = saldo_wn - saldo_ma

        if saldo_bank is None:
            self._wyniki.append(PunktKontroli(
                konto="130",
                punkt="Rachunek bankowy vs wyciąg",
                status=StatusAudytu.OSTRZEZ,
                uwagi="Nie dostarczono wyciągu bankowego. Zweryfikuj ręcznie.",
                wartosc=f"Saldo ZOiS: {saldo_ksiegowy:,.2f} zł",
            ))
            return

        roznica = abs(saldo_ksiegowy - saldo_bank)

        if roznica < Decimal("0.01"):  # Tolerancja zaokrągleń
            self._wyniki.append(PunktKontroli(
                konto="130",
                punkt="Rachunek bankowy vs wyciąg",
                status=StatusAudytu.OK,
                uwagi="Saldo ZOiS zgodne z wyciągiem bankowym.",
                wartosc=f"{saldo_ksiegowy:,.2f} zł",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="130",
                punkt="Rachunek bankowy vs wyciąg",
                status=StatusAudytu.BLAD,
                uwagi=(
                    f"NIEZGODNOŚĆ: ZOiS={saldo_ksiegowy:,.2f} zł | "
                    f"Bank={saldo_bank:,.2f} zł | "
                    f"Różnica={roznica:,.2f} zł. "
                    "Sprawdź nierozliczone czeki/przelewy."
                ),
            ))

    def _weryfikuj_konto_145(self, dane_zois: DaneZOiS) -> None:
        """
        Konto 145 – Środki pieniężne w drodze.
        Reguła: Saldo końcowe MUSI wynosić dokładnie 0.
        Niezerowe saldo oznacza nierozliczone środki w drodze.
        """
        saldo = self._pobierz_saldo(dane_zois, "145")

        if saldo is None:
            self._wyniki.append(PunktKontroli(
                konto="145",
                punkt="Środki pieniężne w drodze = 0",
                status=StatusAudytu.INFO,
                uwagi="Konto 145 nie wystąpiło w ZOiS (dopuszczalne).",
            ))
            return

        saldo_wn, saldo_ma = saldo
        saldo_netto = saldo_wn - saldo_ma

        if saldo_wn == Decimal("0") and saldo_ma == Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="145",
                punkt="Środki pieniężne w drodze = 0",
                status=StatusAudytu.OK,
                uwagi="Saldo konta 145 wynosi 0.",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="145",
                punkt="Środki pieniężne w drodze = 0",
                status=StatusAudytu.BLAD,
                uwagi=(
                    f"Saldo konta 145 ≠ 0! "
                    f"Wn={saldo_wn:,.2f}, Ma={saldo_ma:,.2f} (netto={saldo_netto:,.2f} zł). "
                    "Sprawdź nierozliczone środki w drodze."
                ),
            ))

    def _weryfikuj_konto_200(self, dane_zois: DaneZOiS) -> None:
        """
        Konto 200 – Rozrachunki z odbiorcami.
        Reguła: Saldo Wn > 0 = OK (należność od klienta).
                Saldo Ma > 0 = BŁĄD (klient ma nadpłatę lub brak faktury sprzedaży).
        """
        saldo = self._pobierz_saldo(dane_zois, "200")

        if saldo is None:
            self._wyniki.append(PunktKontroli(
                konto="200",
                punkt="Rozrachunki z odbiorcami – strona salda",
                status=StatusAudytu.BRAK,
                uwagi="CRITICAL: Nie odnaleziono konta 200 w ZOiS.",
            ))
            return

        saldo_wn, saldo_ma = saldo

        if saldo_ma > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="200",
                punkt="Rozrachunki z odbiorcami – strona salda",
                status=StatusAudytu.BLAD,
                uwagi=(
                    f"Saldo Ma={saldo_ma:,.2f} zł – BŁĄD. "
                    "Możliwe przyczyny: brak wystawionej faktury sprzedaży, "
                    "nadpłata klienta lub błąd księgowania. "
                    "Wymagana weryfikacja per analityka."
                ),
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="200",
                punkt="Rozrachunki z odbiorcami – strona salda",
                status=StatusAudytu.OK,
                uwagi=f"Saldo Wn={saldo_wn:,.2f} zł – należności od odbiorców.",
            ))

    def _weryfikuj_konto_202(self, dane_zois: DaneZOiS) -> None:
        """
        Konto 202 – Rozrachunki z dostawcami.
        Reguła: Saldo Ma > 0 = OK (zobowiązanie wobec dostawcy).
                Saldo Wn > 0 = BŁĄD (nadpłata do dostawcy lub brak faktury zakupu).
        """
        saldo = self._pobierz_saldo(dane_zois, "202")

        if saldo is None:
            self._wyniki.append(PunktKontroli(
                konto="202",
                punkt="Rozrachunki z dostawcami – strona salda",
                status=StatusAudytu.BRAK,
                uwagi="Nie odnaleziono konta 202 w ZOiS.",
            ))
            return

        saldo_wn, saldo_ma = saldo

        if saldo_wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="202",
                punkt="Rozrachunki z dostawcami – strona salda",
                status=StatusAudytu.BLAD,
                uwagi=(
                    f"Saldo Wn={saldo_wn:,.2f} zł – BŁĄD. "
                    "Możliwe przyczyny: nadpłata do dostawcy lub brak zaksięgowanej "
                    "faktury zakupu. Wymagana weryfikacja per analityka."
                ),
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="202",
                punkt="Rozrachunki z dostawcami – strona salda",
                status=StatusAudytu.OK,
                uwagi=f"Saldo Ma={saldo_ma:,.2f} zł – zobowiązania wobec dostawców.",
            ))

    def _weryfikuj_konto_230(self, dane_zois: DaneZOiS) -> None:
        """
        Konto 230 – Rozrachunki z tytułu wynagrodzeń.
        Reguła: Saldo Wn → wypłacone, brak listy płac (wymaga wyjaśnienia).
                Saldo Ma → niezapłacone wynagrodzenia (dopuszczalne przy L4 itp.).
        """
        saldo = self._pobierz_saldo(dane_zois, "230")

        if saldo is None:
            self._wyniki.append(PunktKontroli(
                konto="230",
                punkt="Rozrachunki z pracownikami (wynagrodzenia)",
                status=StatusAudytu.INFO,
                uwagi="Konto 230 nie wystąpiło w ZOiS.",
            ))
            return

        saldo_wn, saldo_ma = saldo

        if saldo_wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="230",
                punkt="Rozrachunki z pracownikami (wynagrodzenia)",
                status=StatusAudytu.OSTRZEZ,
                uwagi=(
                    f"Saldo Wn={saldo_wn:,.2f} zł – wypłacono wynagrodzenia "
                    "przed ujęciem listy płac lub nadpłata. "
                    "Sprawdź powiązanie z listą płac."
                ),
            ))
        elif saldo_ma > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="230",
                punkt="Rozrachunki z pracownikami (wynagrodzenia)",
                status=StatusAudytu.OK,
                uwagi=(
                    f"Saldo Ma={saldo_ma:,.2f} zł – niezapłacone wynagrodzenia "
                    "(dopuszczalne, np. wynagrodzenie za XII do wypłaty w I)."
                ),
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="230",
                punkt="Rozrachunki z pracownikami (wynagrodzenia)",
                status=StatusAudytu.OK,
                uwagi="Saldo konta 230 wynosi 0 (wszystkie wynagrodzenia rozliczone).",
            ))

    def _weryfikuj_konto_229(self, dane_zois: DaneZOiS) -> None:
        """
        Konto 229 – Rozrachunki z ZUS.
        Reguła: Saldo Ma = zobowiązanie wobec ZUS (do zapłaty).
                Saldo musi odpowiadać deklaracji ZUS DRA.
        """
        saldo = self._pobierz_saldo(dane_zois, "229")

        if saldo is None:
            self._wyniki.append(PunktKontroli(
                konto="229",
                punkt="Rozrachunki z ZUS – zobowiązanie",
                status=StatusAudytu.BRAK,
                uwagi="Nie odnaleziono konta 229 w ZOiS. Sprawdź strukturę kont ZUS.",
            ))
            return

        saldo_wn, saldo_ma = saldo

        if saldo_wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="229",
                punkt="Rozrachunki z ZUS – zobowiązanie",
                status=StatusAudytu.OSTRZEZ,
                uwagi=(
                    f"Saldo Wn={saldo_wn:,.2f} zł – nadpłata do ZUS lub "
                    "błąd w dekretacji deklaracji DRA. Wyjaśnij z Płatnikiem."
                ),
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="229",
                punkt="Rozrachunki z ZUS – zobowiązanie",
                status=StatusAudytu.OK,
                uwagi=(
                    f"Saldo Ma={saldo_ma:,.2f} zł – zobowiązanie ZUS. "
                    "Porównaj z ostatnią deklaracją DRA."
                ),
            ))

    def _weryfikuj_konto_220(self, dane_zois: DaneZOiS) -> None:
        """
        Konto 220 – Rozrachunki z US (Urząd Skarbowy, podatek dochodowy PIT).
        Reguła: Saldo Ma = zobowiązanie podatkowe (PIT-4R, PIT-8AR).
                Saldo Wn = nadpłata (wymaga weryfikacji).
        """
        saldo = self._pobierz_saldo(dane_zois, "220")

        if saldo is None:
            self._wyniki.append(PunktKontroli(
                konto="220",
                punkt="Rozrachunki z US – PIT (zobowiązanie)",
                status=StatusAudytu.BRAK,
                uwagi="Nie odnaleziono konta 220 w ZOiS.",
            ))
            return

        saldo_wn, saldo_ma = saldo

        if saldo_wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="220",
                punkt="Rozrachunki z US – PIT (zobowiązanie)",
                status=StatusAudytu.OSTRZEZ,
                uwagi=(
                    f"Saldo Wn={saldo_wn:,.2f} zł – nadpłata podatku lub "
                    "brak dekretacji podatku za XII. Sprawdź z deklaracją PIT-4R/8AR."
                ),
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="220",
                punkt="Rozrachunki z US – PIT (zobowiązanie)",
                status=StatusAudytu.OK,
                uwagi=(
                    f"Saldo Ma={saldo_ma:,.2f} zł – zobowiązanie PIT. "
                    "Porównaj z deklaracją PIT-4R / PIT-8AR."
                ),
            ))

    def _weryfikuj_konto_700(self, dane_zois: DaneZOiS) -> None:
        """
        Konto 700 – Przychody ze sprzedaży produktów/towarów/usług.
        Reguła: Saldo Ma MUSI być > 0 (konto przychodowe – zawsze kredytowe).
                Saldo Wn > 0 = BŁĄD (storno lub błędna dekretacja).
        """
        saldo = self._pobierz_saldo(dane_zois, "700")

        if saldo is None:
            self._wyniki.append(PunktKontroli(
                konto="700",
                punkt="Przychody ze sprzedaży – przychodowość",
                status=StatusAudytu.BRAK,
                uwagi="CRITICAL: Nie odnaleziono konta 700 w ZOiS.",
            ))
            return

        saldo_wn, saldo_ma = saldo

        if saldo_wn > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="700",
                punkt="Przychody ze sprzedaży – przychodowość",
                status=StatusAudytu.BLAD,
                uwagi=(
                    f"Saldo Wn={saldo_wn:,.2f} zł – BŁĄD przychodowości! "
                    "Konto 700 jest kontem wynikowym (Ma). "
                    "Saldo Wn może oznaczać błędne storno lub niepoprawną dekretację."
                ),
            ))
        elif saldo_ma > Decimal("0"):
            self._wyniki.append(PunktKontroli(
                konto="700",
                punkt="Przychody ze sprzedaży – przychodowość",
                status=StatusAudytu.OK,
                uwagi=f"Saldo Ma={saldo_ma:,.2f} zł – przychody poprawne.",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="700",
                punkt="Przychody ze sprzedaży – przychodowość",
                status=StatusAudytu.OSTRZEZ,
                uwagi="Konto 700 ma zerowe obroty – brak przychodów ze sprzedaży?",
            ))

    def _weryfikuj_grupe_4(self, dane_zois: DaneZOiS) -> None:
        """
        Konta grupy 4 (400–499) – Koszty rodzajowe.
        Reguła: KAŻDE konto z tej grupy musi mieć WYŁĄCZNIE Saldo Wn > 0.
                Saldo Ma > 0 na koncie grupy 4 = BŁĄD (storno lub błędna dekretacja).

        Konta kosztowe są zawsze debetowe (Wn). Saldo Ma sugeruje błąd.
        """
        konta_grupy_4 = {
            konto: saldo
            for konto, saldo in dane_zois.konta.items()
            if get_grupa(konto) == 4
        }

        if not konta_grupy_4:
            self._wyniki.append(PunktKontroli(
                konto="Grupa 4",
                punkt="Koszty rodzajowe (400–499) – tylko Saldo Wn",
                status=StatusAudytu.BRAK,
                uwagi="Nie odnaleziono żadnych kont z grupy 4 (400–499) w ZOiS.",
            ))
            return

        konta_z_bledem_ma = []
        suma_kosztow = Decimal("0")

        for numer, (saldo_wn, saldo_ma) in konta_grupy_4.items():
            suma_kosztow += saldo_wn
            if saldo_ma > Decimal("0"):
                opis = dane_zois.opisy.get(numer, "")
                konta_z_bledem_ma.append(
                    f"{numer} ({opis}): Ma={saldo_ma:,.2f} zł"
                )

        if konta_z_bledem_ma:
            self._wyniki.append(PunktKontroli(
                konto="Grupa 4",
                punkt="Koszty rodzajowe (400–499) – tylko Saldo Wn",
                status=StatusAudytu.BLAD,
                uwagi=(
                    f"Konta z niepoprawnym Saldem Ma ({len(konta_z_bledem_ma)} szt.): "
                    + "; ".join(konta_z_bledem_ma[:5])
                    + (" ..." if len(konta_z_bledem_ma) > 5 else "")
                ),
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="Grupa 4",
                punkt="Koszty rodzajowe (400–499) – tylko Saldo Wn",
                status=StatusAudytu.OK,
                uwagi=(
                    f"Wszystkie {len(konta_grupy_4)} kont grupy 4 poprawne. "
                    f"Suma kosztów: {suma_kosztow:,.2f} zł."
                ),
            ))

    def _weryfikuj_bilans(self, dane_bilans: Optional[DaneBilansu]) -> None:
        """
        Weryfikacja Bilansu: Aktywa = Pasywa.
        Sprawdzane dla roku bieżącego i ubiegłego.
        """
        if dane_bilans is None:
            self._wyniki.append(PunktKontroli(
                konto="Bilans",
                punkt="Suma bilansowa: Aktywa = Pasywa",
                status=StatusAudytu.BRAK,
                uwagi="Nie dostarczono pliku Bilansu.",
            ))
            return

        TOLERANCJA = Decimal("0.01")  # Tolerancja zaokrągleń

        # ── Rok bieżący ──────────────────────────────────────────────────────
        roznica_biezacy = abs(dane_bilans.aktywa_biezacy - dane_bilans.pasywa_biezacy)

        if roznica_biezacy <= TOLERANCJA:
            self._wyniki.append(PunktKontroli(
                konto="Bilans",
                punkt="Suma bilansowa rok bieżący: Aktywa = Pasywa",
                status=StatusAudytu.OK,
                uwagi=f"Suma bilansowa: {dane_bilans.aktywa_biezacy:,.2f} zł.",
            ))
        else:
            self._wyniki.append(PunktKontroli(
                konto="Bilans",
                punkt="Suma bilansowa rok bieżący: Aktywa = Pasywa",
                status=StatusAudytu.BLAD,
                uwagi=(
                    f"NIEZGODNOŚĆ! Aktywa={dane_bilans.aktywa_biezacy:,.2f} ≠ "
                    f"Pasywa={dane_bilans.pasywa_biezacy:,.2f} | "
                    f"Różnica={roznica_biezacy:,.2f} zł."
                ),
            ))

        # ── Rok ubiegły (dane porównawcze) ───────────────────────────────────
        if dane_bilans.aktywa_ubiegly > Decimal("0") or dane_bilans.pasywa_ubiegly > Decimal("0"):
            roznica_ubiegly = abs(
                dane_bilans.aktywa_ubiegly - dane_bilans.pasywa_ubiegly
            )

            if roznica_ubiegly <= TOLERANCJA:
                self._wyniki.append(PunktKontroli(
                    konto="Bilans",
                    punkt="Suma bilansowa rok ubiegły: Aktywa = Pasywa",
                    status=StatusAudytu.OK,
                    uwagi=f"Dane porównawcze: {dane_bilans.aktywa_ubiegly:,.2f} zł.",
                ))
            else:
                self._wyniki.append(PunktKontroli(
                    konto="Bilans",
                    punkt="Suma bilansowa rok ubiegły: Aktywa = Pasywa",
                    status=StatusAudytu.BLAD,
                    uwagi=(
                        f"NIEZGODNOŚĆ danych porównawczych! "
                        f"Aktywa={dane_bilans.aktywa_ubiegly:,.2f} ≠ "
                        f"Pasywa={dane_bilans.pasywa_ubiegly:,.2f} | "
                        f"Różnica={roznica_ubiegly:,.2f} zł."
                    ),
                ))

    # ─── PARSOWANIE WYCIĄGU BANKOWEGO ─────────────────────────────────────────

    def _parsuj_wyciag_bankowy(self, dane_binarne: bytes, format_pliku: str) -> Decimal:
        """
        Wyciąga saldo końcowe z wyciągu bankowego.

        Obsługuje typowe formaty wyciągów bankowych (XLSX/PDF):
        - Kolumna "Saldo" lub "Saldo końcowe" – ostatni wiersz z kwotą.
        - Wiersz "Saldo końcowe" / "Closing balance" – w tabelach PDF.

        Zwraca saldo jako Decimal (wartość absolutna kwoty).
        """
        KLUCZE_SALDO = [
            "saldo końcowe", "saldo na koniec", "closing balance",
            "balance", "saldo", "stan na"
        ]

        if format_pliku.lower() == "xlsx":
            bufor = io.BytesIO(dane_binarne)
            try:
                df = pd.read_excel(bufor, engine="openpyxl", header=None)
            except Exception as e:
                raise ValueError(f"Błąd odczytu wyciągu XLSX: {e}")

            # Szukaj wiersza z "saldo końcowe"
            for _, wiersz in df.iterrows():
                for komórka in wiersz:
                    if isinstance(komórka, str):
                        if any(k in komórka.lower() for k in KLUCZE_SALDO):
                            # Szukaj kwoty w tym wierszu
                            for val in wiersz:
                                try:
                                    kwota = normalize_currency(val)
                                    if kwota != Decimal("0"):
                                        logger.info(f"Saldo bankowe (XLSX): {kwota:,.2f} zł")
                                        return kwota
                                except (ValueError, TypeError):
                                    pass

            # Fallback: ostatnia numeryczna wartość w kolumnie "Saldo"
            naglowki = df.iloc[0].astype(str).str.lower().tolist()
            for i, naglowek in enumerate(naglowki):
                if any(k in naglowek for k in KLUCZE_SALDO):
                    kolumna = df.iloc[1:, i]
                    for val in reversed(kolumna.tolist()):
                        try:
                            kwota = normalize_currency(val)
                            if kwota != Decimal("0"):
                                return kwota
                        except (ValueError, TypeError):
                            pass

        elif format_pliku.lower() == "pdf":
            if not PDF_AVAILABLE:
                raise ImportError("Wymagana biblioteka 'pdfplumber'.")

            bufor = io.BytesIO(dane_binarne)
            with pdfplumber.open(bufor) as pdf:
                ostatnia_strona = pdf.pages[-1]
                tekst = ostatnia_strona.extract_text() or ""

                for linia in reversed(tekst.splitlines()):
                    linia_lower = linia.lower()
                    if any(k in linia_lower for k in KLUCZE_SALDO):
                        # Wyciągnij kwotę z linii
                        tokeny = linia.split()
                        for token in reversed(tokeny):
                            try:
                                kwota = normalize_currency(token)
                                if kwota != Decimal("0"):
                                    logger.info(f"Saldo bankowe (PDF): {kwota:,.2f} zł")
                                    return kwota
                            except (ValueError, TypeError):
                                pass

        raise ValueError(
            "Nie udało się automatycznie odczytać salda bankowego. "
            "Podaj saldo ręcznie przez parametr 'saldo_bankowe_reczne'."
        )


# =============================================================================
# INTEGRACJA Z FRAPPE – PRZYKŁADOWY SERVER SCRIPT
# =============================================================================

FRAPPE_SERVER_SCRIPT_EXAMPLE = '''
# ─── Frappe Server Script – Kontrola Jakości Zamknięcia Roku ─────────────────
# Podpięty pod: DocType "ZamkniecieRoku" | Trigger: "Uruchom Audyt"
# ─────────────────────────────────────────────────────────────────────────────

import frappe
from symfonia_year_end_auditor import SymfoniaYearEndAuditor

def uruchom_audyt_zamkniecia_roku(doc, method=None):
    """Wywołanie audytora z poziomu Frappe Server Script."""

    audytor = SymfoniaYearEndAuditor()

    # Pobranie plików z załączników dokumentu Frappe (in-memory, bez zapisu)
    def pobierz_zawartosc_pliku(file_url):
        """Pobiera zawartość pliku Frappe jako bytes."""
        if not file_url:
            return None
        file_doc = frappe.get_doc("File", {"file_url": file_url})
        # Odczyt z systemu plików Frappe (site/public lub private)
        sciezka = frappe.get_site_path(
            "public" if not file_doc.is_private else "private",
            "files",
            file_doc.file_name
        )
        with open(sciezka, "rb") as f:
            return f.read()

    try:
        raport = audytor.run_full_audit(
            zois_bytes=pobierz_zawartosc_pliku(doc.zois_file),
            zois_format=doc.zois_format or "xlsx",

            bilans_bytes=pobierz_zawartosc_pliku(doc.bilans_file),
            bilans_format=doc.bilans_format or "xlsx",

            bank_bytes=pobierz_zawartosc_pliku(doc.wyciag_bankowy_file),
            bank_format=doc.wyciag_format or "xlsx",

            nazwa_podmiotu=doc.nazwa_podmiotu or doc.client_name,
            rok=doc.rok_obrachunkowy or 2024,
        )

        # Zapis wyników do pól dokumentu Frappe
        doc.raport_tekstowy    = raport["tekst"]
        doc.liczba_bledow      = raport["podsumowanie"]["bledy"]
        doc.liczba_ostrzezen   = raport["podsumowanie"]["ostrzezenia"]
        doc.status_audytu      = "Błędy" if raport["podsumowanie"]["bledy"] > 0 else "OK"
        doc.data_audytu        = frappe.utils.now()

        # Zapisanie wyników jako Child Table (opcjonalnie)
        doc.wyniki_audytu = []
        for wynik in raport["wyniki"]:
            doc.append("wyniki_audytu", {
                "konto":   wynik["konto"],
                "status":  wynik["status"],
                "punkt":   wynik["punkt"],
                "uwagi":   wynik["uwagi"],
            })

        frappe.msgprint(
            f"Audyt zakończony: {raport['podsumowanie']['bledy']} błędów, "
            f"{raport['podsumowanie']['ostrzezenia']} ostrzeżeń.",
            title="Kontrola Jakości – Wynik"
        )

    except Exception as e:
        frappe.log_error(f"Błąd audytora: {e}", "SymfoniaYearEndAuditor")
        frappe.throw(f"Błąd podczas audytu: {str(e)}")
'''


# =============================================================================
# URUCHOMIENIE LOKALNE (TRYB TESTOWY)
# =============================================================================

if __name__ == "__main__":
    """
    Tryb testowy – symulacja danych ZOiS bez prawdziwych plików.
    Uruchom: python symfonia_year_end_auditor.py
    """
    from decimal import Decimal

    print("=" * 70)
    print("  SymfoniaYearEndAuditor – Tryb Testowy (Dane Syntetyczne)")
    print("=" * 70)

    # ── Symulacja danych ZOiS ────────────────────────────────────────────────
    dane_zois_testowe = DaneZOiS()
    dane_zois_testowe.konta = {
        # (saldo_wn, saldo_ma)
        "130":  (Decimal("125000.00"), Decimal("0")),     # Rachunek bankowy OK
        "145":  (Decimal("500.00"),    Decimal("0")),     # Środki w drodze – BŁĄD
        "200":  (Decimal("0"),         Decimal("3200.00")), # Odbiorca Ma – BŁĄD
        "202":  (Decimal("0"),         Decimal("8750.00")), # Dostawca Ma – OK
        "229":  (Decimal("0"),         Decimal("4200.00")), # ZUS Ma – OK
        "220":  (Decimal("0"),         Decimal("1890.00")), # US Ma – OK
        "230":  (Decimal("0"),         Decimal("12500.00")), # Wynagrodzenia Ma – OK
        "400":  (Decimal("45000.00"),  Decimal("0")),     # Koszty OK
        "401":  (Decimal("8200.00"),   Decimal("100.00")), # Koszty Ma – BŁĄD
        "700":  (Decimal("0"),         Decimal("210000.00")), # Przychody OK
    }
    dane_zois_testowe.opisy = {
        "130": "Rachunek bankowy", "145": "Środki pieniężne w drodze",
        "200": "Rozrachunki z odbiorcami", "202": "Rozrachunki z dostawcami",
        "400": "Zużycie materiałów", "401": "Wynagrodzenia",
        "700": "Sprzedaż usług",
    }

    # ── Symulacja danych Bilansu ─────────────────────────────────────────────
    dane_bilans_testowe = DaneBilansu(
        aktywa_biezacy=Decimal("350000.00"),
        pasywa_biezacy=Decimal("350000.00"),
        aktywa_ubiegly=Decimal("280000.00"),
        pasywa_ubiegly=Decimal("280050.00"),  # Celowo rozbieżny
    )

    # ── Uruchomienie Audytora ────────────────────────────────────────────────
    audytor = SymfoniaYearEndAuditor()

    wyniki = audytor.check_accounting_logic(
        dane_zois=dane_zois_testowe,
        dane_bilans=dane_bilans_testowe,
        saldo_bank=Decimal("125000.00"),  # Zgodne z kontem 130
    )

    raport = audytor.generate_audit_report(
        wyniki,
        nazwa_podmiotu="TESTOWA SP. Z O.O.",
        rok=2024,
    )

    print(raport["tekst"])
