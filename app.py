"""
=============================================================================
Abacus Centrum Księgowe – Kontrola Jakości Zamknięcia Roku
Streamlit Frontend v2.0 (wieloraczkowe rachunki bankowe)
=============================================================================
Flow dwuetapowy:
  1. ANALIZA ZOiS → wykrycie wszystkich analityk konta 130 (rachunków)
  2. UZUPEŁNIENIE WYCIĄGÓW → dla każdego rachunku wyciąg lub ręczne saldo
  3. URUCHOMIENIE AUDYTU → pełna weryfikacja
=============================================================================
"""

from decimal import Decimal

import pandas as pd
import streamlit as st

from symfonia_year_end_auditor import (
    SymfoniaYearEndAuditor,
    DaneZOiS,
    DaneBilansu,
    DaneRZiS,
    DaneKRS,
    WyciagBankowy,
    MIESIACE_PL,
    normalize_currency,
    pobierz_dane_krs,
    wyslij_raport_email,
)

# =============================================================================
# KONFIGURACJA
# =============================================================================

st.set_page_config(
    page_title="Kontrola Zamknięcia Roku | Abacus",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# CSS – STYL ABACUS
# =============================================================================

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* Nagłówek */
.abacus-header {
    background: linear-gradient(135deg, #0d1b2a 0%, #1b2d45 50%, #0d1b2a 100%);
    border-radius: 12px;
    padding: 28px 36px;
    margin-bottom: 24px;
    display: flex; align-items: center; gap: 20px;
    box-shadow: 0 4px 24px rgba(13, 27, 42, 0.35);
    border: 1px solid rgba(255,255,255,0.07);
}
.abacus-logo { font-size: 2.6rem; line-height: 1; }
.abacus-title-block h1 {
    color: #ffffff; font-size: 1.55rem; font-weight: 700;
    margin: 0; letter-spacing: -0.3px;
}
.abacus-title-block p {
    color: #8ca9c9; font-size: 0.82rem; margin: 4px 0 0 0;
    text-transform: uppercase; letter-spacing: 0.3px;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #f4f6f8 !important; border-right: 1px solid #e0e6ed;
}
section[data-testid="stSidebar"] .stMarkdown h3 {
    color: #0d1b2a; font-size: 0.78rem; font-weight: 700;
    text-transform: uppercase; letter-spacing: 1px;
    margin-top: 20px; margin-bottom: 8px;
    padding-bottom: 4px; border-bottom: 2px solid #1b2d45;
}

/* Karty metryk */
.metric-card {
    background: #ffffff; border-radius: 10px; padding: 18px 20px;
    border: 1px solid #e4e9f0; text-align: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}
.metric-card .metric-value { font-size: 2.2rem; font-weight: 700; line-height: 1; margin-bottom: 6px; }
.metric-card .metric-label {
    font-size: 0.78rem; color: #6b7a8d; font-weight: 500;
    text-transform: uppercase; letter-spacing: 0.5px;
}
.metric-ok   { border-top: 4px solid #22c55e; } .metric-ok   .metric-value { color: #16a34a; }
.metric-blad { border-top: 4px solid #ef4444; } .metric-blad .metric-value { color: #dc2626; }
.metric-warn { border-top: 4px solid #f59e0b; } .metric-warn .metric-value { color: #d97706; }
.metric-brak { border-top: 4px solid #94a3b8; } .metric-brak .metric-value { color: #64748b; }

/* Wiersze wyników */
.wynik-row {
    border-radius: 8px; padding: 12px 16px; margin-bottom: 6px;
    border-left: 5px solid; display: flex; align-items: flex-start; gap: 12px;
}
.wynik-row.ok      { background: #f0fdf4; border-color: #22c55e; }
.wynik-row.blad    { background: #fef2f2; border-color: #ef4444; }
.wynik-row.ostrzez { background: #fffbeb; border-color: #f59e0b; }
.wynik-row.brak    { background: #f8fafc; border-color: #94a3b8; }
.wynik-row.info    { background: #eff6ff; border-color: #3b82f6; }
.wynik-icon { font-size: 1.2rem; margin-top: 1px; min-width: 24px; }
.wynik-content .konto-label {
    font-weight: 700; font-size: 0.78rem;
    text-transform: uppercase; letter-spacing: 0.8px; color: #374151;
}
.wynik-content .punkt-text {
    font-size: 0.88rem; color: #1f2937; font-weight: 500; margin: 2px 0;
}
.wynik-content .uwagi-text {
    font-size: 0.81rem; color: #6b7a8d; line-height: 1.45;
}

/* Ocena końcowa */
.ocena-banner {
    border-radius: 10px; padding: 18px 24px; font-size: 1rem;
    font-weight: 600; text-align: center; margin-top: 20px;
}
.ocena-ok   { background: #dcfce7; color: #14532d; border: 1px solid #86efac; }
.ocena-warn { background: #fef9c3; color: #713f12; border: 1px solid #fde047; }
.ocena-blad { background: #fee2e2; color: #7f1d1d; border: 1px solid #fca5a5; }

/* Sekcje */
.section-title {
    font-size: 0.75rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 1.2px; color: #64748b;
    margin: 20px 0 10px; display: flex; align-items: center; gap: 8px;
}
.section-title::after {
    content: ''; flex: 1; height: 1px; background: #e2e8f0;
}

/* Rachunek card */
.rachunek-card {
    background: #ffffff; border: 1px solid #e4e9f0; border-radius: 10px;
    padding: 16px 20px; margin-bottom: 12px;
    border-left: 4px solid #1b2d45;
}
.rachunek-card.uzupelniony { border-left-color: #22c55e; background: #f0fdf4; }
.rachunek-card.brak        { border-left-color: #f59e0b; background: #fffbeb; }

.rachunek-header {
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 10px;
}
.rachunek-number {
    font-weight: 700; color: #0d1b2a; font-size: 0.95rem;
}
.rachunek-saldo {
    font-weight: 600; color: #1b2d45; font-size: 0.9rem;
    background: #eff6ff; padding: 4px 10px; border-radius: 6px;
}
.rachunek-opis {
    color: #6b7a8d; font-size: 0.82rem; margin-bottom: 8px;
}

/* Stepper */
.stepper {
    display: flex; gap: 8px; margin-bottom: 20px;
}
.stepper-item {
    flex: 1; padding: 12px 16px; border-radius: 8px;
    background: #f1f5f9; border: 1px solid #e2e8f0;
    text-align: center;
}
.stepper-item.active {
    background: #0d1b2a; color: #ffffff; border-color: #0d1b2a;
}
.stepper-item.done {
    background: #dcfce7; color: #14532d; border-color: #86efac;
}
.stepper-item .step-num {
    font-weight: 700; font-size: 0.8rem; display: block;
}
.stepper-item .step-label {
    font-size: 0.75rem; margin-top: 2px;
}

#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# NAGŁÓWEK
# =============================================================================

st.markdown("""
<div class="abacus-header">
    <div class="abacus-logo">📊</div>
    <div class="abacus-title-block">
        <h1>Kontrola Jakości – Zamknięcie Roku Obrachunkowego</h1>
        <p>Abacus Centrum Księgowe Puławy &nbsp;·&nbsp; ZOiS · Bilans · RZiS · Wyciągi Bankowe</p>
    </div>
</div>
""", unsafe_allow_html=True)


# =============================================================================
# INICJALIZACJA STANU
# =============================================================================

def reset_state():
    """Reset wszystkich danych audytu."""
    for k in ["dane_zois", "dane_bilans", "dane_rzis", "dane_krs", "konta_bankowe",
              "wyciagi", "raport", "etap"]:
        if k in st.session_state:
            del st.session_state[k]

if "etap" not in st.session_state:
    st.session_state["etap"] = 1  # 1=ZOiS, 2=wyciągi, 3=raport

if "wyciagi" not in st.session_state:
    st.session_state["wyciagi"] = {}  # klucz: numer_ksiegowy → WyciagBankowy


# =============================================================================
# SIDEBAR – DANE PODMIOTU
# =============================================================================

with st.sidebar:
    st.markdown("### 🏢 Podmiot")
    nazwa_podmiotu = st.text_input(
        "Nazwa podmiotu",
        value=st.session_state.get("nazwa_podmiotu", ""),
        placeholder="np. FIRMA SP. Z O.O.",
        key="input_nazwa",
    )
    st.session_state["nazwa_podmiotu"] = nazwa_podmiotu

    # Rok obrachunkowy – domyślnie poprzedni rok (za który robi się sprawozdanie).
    # Oferujemy 5 lat wstecz, żeby można było robić audyty historyczne.
    from datetime import date as _date
    rok_biezacy = _date.today().year
    dostepne_lata = list(range(rok_biezacy, rok_biezacy - 6, -1))
    rok = st.selectbox(
        "Rok obrachunkowy",
        options=dostepne_lata,
        index=1,  # domyślnie rok_biezacy - 1 (np. 2025 gdy jesteśmy w 2026)
        key="input_rok",
        help="Rok za który przeprowadzany jest audyt. Domyślnie rok poprzedni.",
    )
    st.session_state["rok"] = rok

    st.markdown("---")
    st.markdown("### ⚙️ Akcje")

    if st.button("🔄 Rozpocznij od nowa", use_container_width=True):
        reset_state()
        st.rerun()

    st.markdown("---")
    st.markdown("### 📊 Postęp audytu")

    # Stepper indicator
    e = st.session_state["etap"]
    def krok_klasa(n):
        if e > n: return "done"
        if e == n: return "active"
        return ""

    st.markdown(f"""
    <div style="display:flex; flex-direction:column; gap:6px;">
        <div class="stepper-item {krok_klasa(1)}" style="text-align:left;padding:8px 12px;">
            <span class="step-num">1. Analiza ZOiS</span>
            <span class="step-label">Wykrycie rachunków</span>
        </div>
        <div class="stepper-item {krok_klasa(2)}" style="text-align:left;padding:8px 12px;">
            <span class="step-num">2. Wyciągi</span>
            <span class="step-label">Uzupełnienie sald</span>
        </div>
        <div class="stepper-item {krok_klasa(3)}" style="text-align:left;padding:8px 12px;">
            <span class="step-num">3. Raport</span>
            <span class="step-label">Wynik audytu</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


# =============================================================================
# FUNKCJE POMOCNICZE
# =============================================================================

def pobierz_format(plik) -> str:
    if plik is None:
        return "xlsx"
    return "pdf" if plik.name.lower().endswith(".pdf") else "xlsx"


def renderuj_wynik(wynik: dict, indeks: int = 0):
    mapa = {
        "✅ OK":            ("ok",      "✅"),
        "❌ BŁĄD":          ("blad",    "❌"),
        "⚠️  OSTRZEŻENIE":  ("ostrzez", "⚠️"),
        "🔍 BRAK DANYCH":   ("brak",    "🔍"),
        "ℹ️  INFO":          ("info",    "ℹ️"),
    }
    klasa, ikona = mapa.get(wynik["status"], ("info", "ℹ️"))
    wartosc_html = (
        f'<span style="font-size:0.78rem;color:#4b5563;margin-left:6px;">'
        f'[{wynik["wartosc"]}]</span>' if wynik.get("wartosc") else ""
    )
    st.markdown(f"""
    <div class="wynik-row {klasa}">
        <div class="wynik-icon">{ikona}</div>
        <div class="wynik-content">
            <div class="konto-label">Konto {wynik["konto"]}</div>
            <div class="punkt-text">{wynik["punkt"]}{wartosc_html}</div>
            <div class="uwagi-text">{wynik["uwagi"]}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Pole komentarza dla błędów i ostrzeżeń (wymagane do wysyłki maila)
    status = wynik["status"]
    wymaga_komentarza = ("BŁĄD" in status) or ("OSTRZEŻENIE" in status)
    if wymaga_komentarza:
        klucz = f"komentarz_{indeks}_{wynik['konto']}_{wynik['punkt'][:40]}"
        komentarze = st.session_state.setdefault("komentarze", {})
        komentarz = st.text_area(
            "💬 Komentarz osoby księgującej (wymagany)",
            value=komentarze.get(klucz, ""),
            key=klucz,
            height=70,
            placeholder="Opisz podjęte działania lub wyjaśnienie rozbieżności…",
            label_visibility="visible",
        )
        komentarze[klucz] = komentarz


# =============================================================================
# ETAP 1 – WGRANIE I ANALIZA ZOiS
# =============================================================================

if st.session_state["etap"] == 1:
    st.markdown('<div class="section-title">Etap 1 · Wgranie Zestawienia Obrotów i Sald</div>',
                unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("""
        **Wgraj sprawozdania finansowe z Symfonii:**

        1. **ZOiS** – Zestawienie Obrotów i Sald (wymagane)
        2. **Bilans** – struktura aktywów/pasywów + wynik netto
        3. **RZiS** – Rachunek Zysków i Strat (wariant porównawczy)

        System porówna je krzyżowo i wykryje wszystkie rachunki bankowe
        (analityki konta 130) do uzupełnienia w następnym kroku.
        """)

        plik_zois = st.file_uploader(
            "📋 Zestawienie Obrotów i Sald (XLSX lub PDF)",
            type=["xlsx", "xls", "pdf"],
            key="upload_zois",
        )

        plik_bilans = st.file_uploader(
            "📊 Bilans (XLSX lub PDF)",
            type=["xlsx", "xls", "pdf"],
            key="upload_bilans",
        )

        plik_rzis = st.file_uploader(
            "📈 Rachunek Zysków i Strat (XLSX lub PDF)",
            type=["xlsx", "xls", "pdf"],
            key="upload_rzis",
        )

        # ── Numer KRS (opcjonalny – do weryfikacji kapitału zakładowego) ─────
        numer_krs = st.text_input(
            "🏛️ Numer KRS (opcjonalnie)",
            placeholder="np. 0000640431",
            max_chars=10,
            help=("10-cyfrowy numer KRS. Jeśli podany, system pobierze kapitał "
                  "zakładowy i zweryfikuje zgodność z ewidencją."),
            key="input_krs",
        )

        tryb_testowy = st.checkbox(
            "🧪 Tryb diagnostyczny",
            help=("Uruchamia audyt na minimalnych danych syntetycznych. "
                  "Służy wyłącznie do sprawdzenia czy aplikacja działa "
                  "(parsery, reguły, raport). Nie wgrywa żadnych plików, "
                  "nie łączy się z KRS.")
        )

        if st.button(
            "🔍 Analizuj sprawozdania →",
            type="primary",
            use_container_width=True,
            disabled=(not tryb_testowy and plik_zois is None),
        ):
            audytor = SymfoniaYearEndAuditor()

            try:
                if tryb_testowy:
                    # ═══ DANE DIAGNOSTYCZNE ═════════════════════════════════
                    # Minimalny zbiór danych do sprawdzenia czy aplikacja żyje.
                    # NIE jest to pełna demonstracja możliwości. Służy tylko
                    # do diagnostyki po deployu: czy parsery, reguły i raport
                    # działają end-to-end. Wynikiem ma być raport który się
                    # renderuje – spójność danych nie ma znaczenia.
                    dz = DaneZOiS()
                    dz.konta = {
                        "130": (Decimal("100000.00"), Decimal("0")),
                        "200": (Decimal("10000.00"), Decimal("0")),
                        "400": (Decimal("50000.00"), Decimal("0")),
                        "700": (Decimal("0"),        Decimal("75000.00")),
                    }
                    dz.konta_analityki = {
                        "130": (Decimal("100000.00"), Decimal("0")),
                    }
                    dz.opisy = {
                        "130": "Rachunek bankowy [DIAG]",
                        "200": "Odbiorcy [DIAG]",
                        "400": "Koszty rodzajowe [DIAG]",
                        "700": "Przychody [DIAG]",
                    }
                    st.session_state["dane_zois"] = dz
                    st.session_state["dane_bilans"] = None
                    st.session_state["dane_rzis"] = None
                    st.session_state["dane_krs"] = None
                    st.session_state["nazwa_podmiotu"] = "[TRYB DIAGNOSTYCZNY]"
                    st.session_state["tryb_testowy"] = True
                else:
                    # Parsowanie prawdziwego ZOiS
                    dz = audytor.parsuj_zois(plik_zois.read(), pobierz_format(plik_zois))
                    st.session_state["dane_zois"] = dz
                    st.session_state["tryb_testowy"] = False

                    # Bilans (opcjonalnie)
                    if plik_bilans:
                        db = audytor.parsuj_bilans(plik_bilans.read(), pobierz_format(plik_bilans))
                        st.session_state["dane_bilans"] = db
                    else:
                        st.session_state["dane_bilans"] = None

                    # RZiS (opcjonalnie)
                    if plik_rzis:
                        dr = audytor.parsuj_rzis(plik_rzis.read(), pobierz_format(plik_rzis))
                        st.session_state["dane_rzis"] = dr
                    else:
                        st.session_state["dane_rzis"] = None

                    # KRS (opcjonalnie – pobranie z API Ministerstwa Sprawiedliwości)
                    if numer_krs and numer_krs.strip():
                        with st.spinner("Pobieram dane z rejestru KRS…"):
                            dk = pobierz_dane_krs(numer_krs.strip())
                            st.session_state["dane_krs"] = dk
                            if dk.blad:
                                st.warning(f"⚠️ KRS: {dk.blad}")
                            else:
                                st.success(
                                    f"✅ KRS pobrano: {dk.nazwa[:60]} | "
                                    f"kapitał {dk.kapital_zakladowy:,.2f} zł"
                                )
                    else:
                        st.session_state["dane_krs"] = None

                # Wykrycie rachunków
                st.session_state["konta_bankowe"] = dz.pobierz_konta_bankowe()
                st.session_state["etap"] = 2
                st.rerun()

            except Exception as e:
                st.error(f"❌ Błąd parsowania: {e}")

    with col2:
        st.markdown("""
        <div class="metric-card" style="text-align:left;">
            <div style="font-weight:700; font-size:0.95rem; color:#1b2d45; margin-bottom:10px;">
                💡 Co robi ten etap?
            </div>
            <div style="font-size:0.82rem; color:#4b5563; line-height:1.5;">
                System przetwarza trzy sprawozdania:
                <ul style="margin:8px 0 0 0; padding-left:18px;">
                    <li><strong>ZOiS</strong> → konta syntetyczne + analityki (130-X)</li>
                    <li><strong>Bilans</strong> → aktywa, pasywa, wynik netto</li>
                    <li><strong>RZiS</strong> → pozycje A-L (wariant porównawczy)</li>
                </ul>
                Po przetworzeniu przejdziemy do etapu wyciągów bankowych.
            </div>
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# ETAP 2 – UZUPEŁNIENIE WYCIĄGÓW PER RACHUNEK
# =============================================================================

elif st.session_state["etap"] == 2:
    konta_bankowe = st.session_state.get("konta_bankowe", [])
    wyciagi_dict = st.session_state.get("wyciagi", {})

    st.markdown(
        f'<div class="section-title">Etap 2 · Wyciągi bankowe '
        f'({len(konta_bankowe)} rachunek{"ów" if len(konta_bankowe) != 1 else ""})</div>',
        unsafe_allow_html=True
    )

    if not konta_bankowe:
        st.warning(
            "⚠️ Nie wykryto rachunków bankowych w ZOiS (brak konta 130). "
            "Możesz pominąć ten etap."
        )
    else:
        st.markdown(f"""
        **Wykryto {len(konta_bankowe)} rachunek(ów) bankowych w ZOiS.**
        Dla każdego rachunku wgraj wyciąg lub wpisz saldo ręcznie.
        Rachunki bez wyciągu zostaną oznaczone w raporcie.

        💡 *Jeśli ostatnia operacja na rachunku była przed grudniem – system to wykryje
        i wskaże ten fakt w raporcie (np. „rachunek zamknięty operacjami z kwietnia").*
        """)

        audytor = SymfoniaYearEndAuditor()

        for numer_ks, opis, saldo_zois in konta_bankowe:
            # Sprawdź czy rachunek ma już uzupełniony wyciąg
            w_istniejacy = wyciagi_dict.get(numer_ks)
            klasa = "uzupelniony" if w_istniejacy else "brak"

            with st.container():
                st.markdown(f"""
                <div class="rachunek-card {klasa}">
                    <div class="rachunek-header">
                        <div class="rachunek-number">🏦 Konto {numer_ks}</div>
                        <div class="rachunek-saldo">Saldo ZOiS: {saldo_zois:,.2f} zł</div>
                    </div>
                    <div class="rachunek-opis">{opis}</div>
                </div>
                """, unsafe_allow_html=True)

                col_u, col_r = st.columns([1, 1])

                # ── Wariant A: wgranie wyciągu ───────────────────────────────
                with col_u:
                    st.markdown("**Wariant A:** Wgraj wyciąg bankowy")
                    plik_w = st.file_uploader(
                        "Wyciąg (PDF/XLSX)",
                        type=["pdf", "xlsx", "xls"],
                        key=f"w_{numer_ks}",
                        label_visibility="collapsed",
                        help="Po wgraniu plik zostanie automatycznie sparsowany.",
                    )
                    # Auto-parsowanie: jeśli plik jest wgrany i jeszcze nie
                    # zapisaliśmy dla tego rachunku ani nie mamy flagi że był
                    # już sparsowany ten konkretny plik – parsujemy teraz.
                    plik_id_key = f"_parsed_file_{numer_ks}"
                    plik_file_id = plik_w.file_id if plik_w else None

                    if plik_w and st.session_state.get(plik_id_key) != plik_file_id:
                        try:
                            w = audytor.parsuj_wyciag(
                                numer_konta_ksiegowego=numer_ks,
                                dane_binarne=plik_w.read(),
                                format_pliku=pobierz_format(plik_w),
                            )
                            wyciagi_dict[numer_ks] = w
                            st.session_state["wyciagi"] = wyciagi_dict
                            st.session_state[plik_id_key] = plik_file_id
                            st.rerun()
                        except Exception as e:
                            st.error(f"Błąd parsowania: {e}")

                # ── Wariant B: ręczne saldo ──────────────────────────────────
                with col_r:
                    st.markdown("**Wariant B:** Wpisz ręcznie")

                    rc1, rc2, rc3 = st.columns([2, 2, 1])
                    with rc1:
                        saldo_txt = st.text_input(
                            "Saldo końcowe (zł)",
                            key=f"s_{numer_ks}",
                            placeholder="np. 125 000,00",
                            label_visibility="collapsed",
                        )
                    with rc2:
                        miesiac = st.selectbox(
                            "Miesiąc ostatniej op.",
                            options=[
                                (None, "—"), (1, "styczeń"), (2, "luty"),
                                (3, "marzec"), (4, "kwiecień"), (5, "maj"),
                                (6, "czerwiec"), (7, "lipiec"), (8, "sierpień"),
                                (9, "wrzesień"), (10, "październik"),
                                (11, "listopad"), (12, "grudzień"),
                            ],
                            format_func=lambda x: x[1],
                            key=f"m_{numer_ks}",
                            index=12,  # domyślnie grudzień
                            label_visibility="collapsed",
                        )
                    with rc3:
                        if st.button(
                            "✓",
                            key=f"btn_r_{numer_ks}",
                            use_container_width=True,
                            help="Zapisz ręcznie wpisane dane",
                        ):
                            try:
                                saldo = normalize_currency(saldo_txt) if saldo_txt else Decimal("0")
                                wyciagi_dict[numer_ks] = WyciagBankowy(
                                    numer_konta_ksiegowego=numer_ks,
                                    saldo_koncowe=saldo,
                                    rok_ostatniej_operacji=st.session_state["rok"],
                                    miesiac_ostatniej_operacji=miesiac[0] if miesiac[0] else None,
                                    wgrany_plik=False,
                                )
                                st.session_state["wyciagi"] = wyciagi_dict
                                st.rerun()
                            except ValueError:
                                st.error("Niepoprawne saldo")

                # ── Status rachunku ──────────────────────────────────────────
                if w_istniejacy:
                    okres = w_istniejacy.okres_opisowy
                    zrodlo = "📄 z wyciągu" if w_istniejacy.wgrany_plik else "✏️ ręcznie"
                    bank_info = f" · {w_istniejacy.bank_nazwa}" if w_istniejacy.bank_nazwa else ""

                    # Czy saldo się zgadza?
                    roznica = abs(saldo_zois - w_istniejacy.saldo_koncowe)
                    if roznica < Decimal("0.01"):
                        status_saldo = "✅ Saldo zgodne"
                        kolor = "#16a34a"
                    else:
                        status_saldo = f"❌ Różnica: {roznica:,.2f} zł"
                        kolor = "#dc2626"

                    st.markdown(f"""
                    <div style="background:#eff6ff; border:1px solid #bfdbfe; border-radius:6px;
                                padding:8px 12px; margin:8px 0; font-size:0.83rem;">
                        <strong>Uzupełniono {zrodlo}:</strong>
                        Saldo wyciągu: <strong>{w_istniejacy.saldo_koncowe:,.2f} zł</strong>
                        &middot; Okres: <strong>{okres}</strong>{bank_info}
                        &middot; <span style="color:{kolor}; font-weight:600;">{status_saldo}</span>
                    </div>
                    """, unsafe_allow_html=True)

                    if st.button(f"🗑 Usuń", key=f"del_{numer_ks}"):
                        del wyciagi_dict[numer_ks]
                        st.session_state["wyciagi"] = wyciagi_dict
                        st.rerun()

                st.markdown("<br>", unsafe_allow_html=True)

    # ── Akcje końcowe etapu 2 ────────────────────────────────────────────────
    st.markdown("---")

    kol_a, kol_b, kol_c = st.columns([1, 1, 1])

    with kol_a:
        if st.button("← Cofnij do ZOiS", use_container_width=True):
            st.session_state["etap"] = 1
            st.rerun()

    with kol_b:
        liczba_uzup = len(wyciagi_dict)
        liczba_braku = len(konta_bankowe) - liczba_uzup
        st.info(
            f"✔ Uzupełniono: **{liczba_uzup}/{len(konta_bankowe)}** "
            f"| Brak: **{liczba_braku}**"
        )

    with kol_c:
        if st.button(
            "🚀 Uruchom audyt →",
            type="primary",
            use_container_width=True,
        ):
            # Uruchomienie pełnego audytu
            audytor = SymfoniaYearEndAuditor()
            try:
                wyniki = audytor.check_accounting_logic(
                    dane_zois=st.session_state["dane_zois"],
                    dane_bilans=st.session_state.get("dane_bilans"),
                    dane_rzis=st.session_state.get("dane_rzis"),
                    dane_krs=st.session_state.get("dane_krs"),
                    wyciagi=list(wyciagi_dict.values()),
                    rok_obrachunkowy=st.session_state["rok"],
                )
                raport = audytor.generate_audit_report(
                    wyniki,
                    nazwa_podmiotu=st.session_state.get("nazwa_podmiotu")
                                   or "Podmiot (nieznany)",
                    rok=st.session_state["rok"],
                )
                st.session_state["raport"] = raport
                st.session_state["etap"] = 3
                st.rerun()
            except Exception as e:
                st.error(f"Błąd: {e}")


# =============================================================================
# ETAP 3 – RAPORT
# =============================================================================

elif st.session_state["etap"] == 3:
    raport = st.session_state["raport"]
    podsum = raport["podsumowanie"]
    wyniki = raport["wyniki"]
    podmiot = podsum["podmiot"]
    rok_disp = podsum["rok"]

    st.markdown(
        f'<div class="section-title">Wyniki audytu · {podmiot} · {rok_disp}</div>',
        unsafe_allow_html=True
    )

    # Metryki
    k1, k2, k3, k4 = st.columns(4)
    for kol, (n, v, etykieta) in zip(
        [k1, k2, k3, k4],
        [
            ("metric-ok",   podsum["ok"],          "✅ Poprawnych"),
            ("metric-blad", podsum["bledy"],       "❌ Błędów"),
            ("metric-warn", podsum["ostrzezenia"], "⚠️ Ostrzeżeń"),
            ("metric-brak", podsum["brak_danych"], "🔍 Brak danych"),
        ]
    ):
        with kol:
            st.markdown(f"""
            <div class="metric-card {n}">
                <div class="metric-value">{v}</div>
                <div class="metric-label">{etykieta}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Ocena końcowa
    if podsum["bledy"] == 0 and podsum["ostrzezenia"] == 0:
        kl, tk = "ocena-ok", "🎉 DANE SPÓJNE – gotowe do badania sprawozdania."
    elif podsum["bledy"] == 0:
        kl, tk = "ocena-warn", "🟡 WYMAGA WYJAŚNIENIA – ostrzeżenia do weryfikacji."
    else:
        kl, tk = "ocena-blad", "🔴 DANE NIESPÓJNE – wymagane korekty."

    st.markdown(f'<div class="ocena-banner {kl}">{tk}</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # Tabela + panel boczny
    col_l, col_r = st.columns([2, 1])

    with col_l:
        st.markdown('<div class="section-title">Szczegółowe wyniki</div>',
                    unsafe_allow_html=True)

        f1, f2 = st.columns([3, 1])
        with f1:
            filtr = st.multiselect(
                "Status",
                options=["✅ OK", "❌ BŁĄD", "⚠️  OSTRZEŻENIE", "🔍 BRAK DANYCH", "ℹ️  INFO"],
                default=["❌ BŁĄD", "⚠️  OSTRZEŻENIE", "🔍 BRAK DANYCH"],
                label_visibility="collapsed",
            )
        with f2:
            wsz = st.checkbox("Wszystkie", value=False)

        do_pokazania = wyniki if wsz else [w for w in wyniki if w["status"] in filtr]

        if not do_pokazania:
            st.success("Brak wyników dla wybranych filtrów.")
        else:
            for idx, w in enumerate(do_pokazania):
                renderuj_wynik(w, indeks=idx)

    with col_r:
        st.markdown('<div class="section-title">Eksport</div>', unsafe_allow_html=True)

        # TXT
        st.download_button(
            label="⬇️ Raport TXT",
            data=raport["tekst"].encode("utf-8"),
            file_name=f"audyt_{podmiot.replace(' ', '_')}_{rok_disp}.txt",
            mime="text/plain",
            use_container_width=True,
        )

        # CSV
        df_eks = pd.DataFrame(wyniki)[["konto", "status", "punkt", "uwagi", "wartosc"]]
        df_eks.columns = ["Konto", "Status", "Punkt", "Uwagi", "Wartość"]
        csv = df_eks.to_csv(index=False, sep=";", encoding="utf-8-sig")
        st.download_button(
            label="⬇️ Wyniki CSV",
            data=csv.encode("utf-8"),
            file_name=f"audyt_{podmiot.replace(' ', '_')}_{rok_disp}.csv",
            mime="text/csv",
            use_container_width=True,
        )

        # Wykres
        try:
            import plotly.graph_objects as go
            total = sum([podsum["ok"], podsum["bledy"],
                         podsum["ostrzezenia"], podsum["brak_danych"]])
            if total > 0:
                fig = go.Figure(data=[go.Pie(
                    labels=["OK", "Błędy", "Ostrzeżenia", "Brak"],
                    values=[podsum["ok"], podsum["bledy"],
                            podsum["ostrzezenia"], podsum["brak_danych"]],
                    marker_colors=["#22c55e", "#ef4444", "#f59e0b", "#94a3b8"],
                    hole=0.55, textinfo="percent", showlegend=False,
                )])
                fig.update_layout(
                    margin=dict(t=10, b=10, l=10, r=10), height=220,
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            pass

        with st.expander("📄 Raport tekstowy"):
            st.text(raport["tekst"])

        # ═══ WYSYŁKA DO GŁÓWNEJ KSIĘGOWEJ ════════════════════════════════════
        st.markdown("---")
        st.markdown(
            '<div class="section-title">📧 Przekazanie do Głównej Księgowej</div>',
            unsafe_allow_html=True
        )

        # Zbieramy wszystkie błędy i ostrzeżenia wymagające komentarza
        wymagajace = [
            w for w in wyniki
            if ("BŁĄD" in w["status"]) or ("OSTRZEŻENIE" in w["status"])
        ]

        if not wymagajace:
            st.success(
                "🎉 Brak błędów i ostrzeżeń – raport można przekazać bez komentarzy."
            )
            przekazywalny = True
            brakujace_komentarze = []
        else:
            # Sprawdzamy czy wszystkie komentarze są wypełnione
            # (komentarze trafiają do session_state["komentarze"] w renderuj_wynik,
            # ale user może nie wyświetlił wszystkich pozycji – sprawdzamy
            # wszystkie wymagające, nawet te przefiltrowane)
            komentarze = st.session_state.get("komentarze", {})
            brakujace_komentarze = []
            for idx, w in enumerate(wymagajace):
                # Klucz nie zawsze będzie w state jeśli użytkownik przefiltrował
                # i nie widział danego wyniku – sprawdzamy czy istnieje NIEPUSTY
                # komentarz którego klucz kończy się na ten sam konto+punkt
                wzor = f"_{w['konto']}_{w['punkt'][:40]}"
                znaleziono = any(
                    k.endswith(wzor) and (v or "").strip()
                    for k, v in komentarze.items()
                )
                if not znaleziono:
                    brakujace_komentarze.append((w["konto"], w["punkt"]))

            if brakujace_komentarze:
                st.warning(
                    f"⚠️ Uzupełnij komentarze ({len(brakujace_komentarze)} "
                    f"z {len(wymagajace)}) dla wszystkich błędów i ostrzeżeń. "
                    "Brakujące pozycje są widoczne w filtrach statusu – "
                    "zaznacz oba filtry BŁĄD i OSTRZEŻENIE aby zobaczyć "
                    "wszystkie pola komentarza."
                )
                with st.expander(f"Lista pozycji bez komentarza ({len(brakujace_komentarze)})"):
                    for konto, punkt in brakujace_komentarze:
                        st.markdown(f"- **{konto}** · {punkt}")
                przekazywalny = False
            else:
                st.success(
                    f"✅ Wszystkie komentarze uzupełnione "
                    f"({len(wymagajace)}/{len(wymagajace)}). "
                    "Raport jest gotowy do przekazania."
                )
                przekazywalny = True

        # Przycisk wysyłki
        adres_odbiorcy = "spraw_przyg@abacus24.pl"
        if st.button(
            f"📧 Wyślij do Głównej Księgowej ({adres_odbiorcy})",
            type="primary",
            use_container_width=True,
            disabled=not przekazywalny,
        ):
            try:
                haslo_smtp = st.secrets.get("SMTP_PASSWORD", "")
            except Exception:
                haslo_smtp = ""

            if not haslo_smtp:
                st.error(
                    "❌ Brak konfiguracji SMTP. Administrator musi dodać sekret "
                    "`SMTP_PASSWORD` w ustawieniach Streamlit Cloud "
                    "(Manage app → Settings → Secrets)."
                )
            else:
                # Zbuduj treść maila: raport + sekcja komentarzy
                komentarze = st.session_state.get("komentarze", {})
                linie = [
                    f"Raport kontroli jakości danych przed zamknięciem roku",
                    f"Podmiot: {podmiot}",
                    f"Rok obrachunkowy: {rok_disp}",
                    "",
                    "=" * 70,
                    "PODSUMOWANIE",
                    "=" * 70,
                    f"✅ Poprawne: {podsum['ok']}",
                    f"❌ Błędy krytyczne: {podsum['bledy']}",
                    f"⚠️  Ostrzeżenia: {podsum['ostrzezenia']}",
                    f"🔍 Brak danych: {podsum['brak_danych']}",
                    "",
                ]

                if wymagajace:
                    linie.extend([
                        "=" * 70,
                        "KOMENTARZE OSOBY KSIĘGUJĄCEJ",
                        "=" * 70,
                        "",
                    ])
                    for w in wymagajace:
                        wzor = f"_{w['konto']}_{w['punkt'][:40]}"
                        komentarz_tekst = next(
                            (v for k, v in komentarze.items()
                             if k.endswith(wzor) and (v or "").strip()),
                            "(brak)"
                        )
                        linie.extend([
                            f"Konto: {w['konto']}",
                            f"Punkt: {w['punkt']}",
                            f"Status: {w['status']}",
                            f"Uwagi systemu: {w['uwagi']}",
                            f"Komentarz księgowej: {komentarz_tekst}",
                            "-" * 70,
                            "",
                        ])

                linie.extend([
                    "=" * 70,
                    "PEŁNY RAPORT AUDYTU",
                    "=" * 70,
                    "",
                    raport["tekst"],
                ])

                tresc = "\n".join(linie)
                temat = f"Raport zamknięcia roku {rok_disp} – {podmiot}"

                with st.spinner("Wysyłanie…"):
                    sukces, komunikat = wyslij_raport_email(
                        nadawca=adres_odbiorcy,
                        odbiorca=adres_odbiorcy,
                        haslo=haslo_smtp,
                        temat=temat,
                        tresc_tekstowa=tresc,
                        serwer_smtp="mail.abacus24.pl",
                        port=465,
                    )

                if sukces:
                    st.success(f"✅ {komunikat} Raport trafił do Głównej Księgowej.")
                    st.balloons()
                else:
                    st.error(f"❌ Nie udało się wysłać: {komunikat}")

        st.markdown("---")
        if st.button("↻ Wróć do wyciągów", use_container_width=True):
            st.session_state["etap"] = 2
            st.rerun()
