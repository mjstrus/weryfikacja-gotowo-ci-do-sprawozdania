"""
=============================================================================
Abacus Centrum Księgowe – Kontrola Jakości Zamknięcia Roku
Streamlit Frontend dla SymfoniaYearEndAuditor
=============================================================================
"""

import io
from decimal import Decimal

import streamlit as st
import pandas as pd

# Import modułu audytora (musi być w tym samym folderze)
from symfonia_year_end_auditor import (
    SymfoniaYearEndAuditor,
    DaneZOiS,
    DaneBilansu,
    StatusAudytu,
    normalize_currency,
)

# =============================================================================
# KONFIGURACJA STRONY
# =============================================================================

st.set_page_config(
    page_title="Kontrola Zamknięcia Roku | Abacus",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# CSS – STYL ABACUS (dark navy header, light gray sidebar)
# =============================================================================

st.markdown("""
<style>
/* ── Import czcionki ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Globalne ── */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* ── Nagłówek główny (dark navy gradient) ── */
.abacus-header {
    background: linear-gradient(135deg, #0d1b2a 0%, #1b2d45 50%, #0d1b2a 100%);
    border-radius: 12px;
    padding: 28px 36px;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    gap: 20px;
    box-shadow: 0 4px 24px rgba(13, 27, 42, 0.35);
    border: 1px solid rgba(255,255,255,0.07);
}

.abacus-logo {
    font-size: 2.6rem;
    line-height: 1;
}

.abacus-title-block h1 {
    color: #ffffff;
    font-size: 1.55rem;
    font-weight: 700;
    margin: 0;
    letter-spacing: -0.3px;
}

.abacus-title-block p {
    color: #8ca9c9;
    font-size: 0.82rem;
    margin: 4px 0 0 0;
    font-weight: 400;
    letter-spacing: 0.3px;
    text-transform: uppercase;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #f4f6f8 !important;
    border-right: 1px solid #e0e6ed;
}

section[data-testid="stSidebar"] .stMarkdown h3 {
    color: #0d1b2a;
    font-size: 0.78rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-top: 20px;
    margin-bottom: 8px;
    padding-bottom: 4px;
    border-bottom: 2px solid #1b2d45;
}

section[data-testid="stSidebar"] .stMarkdown hr {
    border-color: #d0d9e3;
    margin: 12px 0;
}

/* ── Karty metryk ── */
.metric-card {
    background: #ffffff;
    border-radius: 10px;
    padding: 18px 20px;
    border: 1px solid #e4e9f0;
    text-align: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    transition: transform 0.15s ease, box-shadow 0.15s ease;
}

.metric-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 16px rgba(0,0,0,0.08);
}

.metric-card .metric-value {
    font-size: 2.2rem;
    font-weight: 700;
    line-height: 1;
    margin-bottom: 6px;
}

.metric-card .metric-label {
    font-size: 0.78rem;
    color: #6b7a8d;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.metric-ok    { border-top: 4px solid #22c55e; }
.metric-blad  { border-top: 4px solid #ef4444; }
.metric-warn  { border-top: 4px solid #f59e0b; }
.metric-brak  { border-top: 4px solid #94a3b8; }
.metric-ok    .metric-value { color: #16a34a; }
.metric-blad  .metric-value { color: #dc2626; }
.metric-warn  .metric-value { color: #d97706; }
.metric-brak  .metric-value { color: #64748b; }

/* ── Wyniki – wiersze tabeli ── */
.wynik-row {
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 6px;
    border-left: 5px solid;
    display: flex;
    align-items: flex-start;
    gap: 12px;
}

.wynik-row.ok       { background: #f0fdf4; border-color: #22c55e; }
.wynik-row.blad     { background: #fef2f2; border-color: #ef4444; }
.wynik-row.ostrzez  { background: #fffbeb; border-color: #f59e0b; }
.wynik-row.brak     { background: #f8fafc; border-color: #94a3b8; }
.wynik-row.info     { background: #eff6ff; border-color: #3b82f6; }

.wynik-icon { font-size: 1.2rem; margin-top: 1px; min-width: 24px; }

.wynik-content .konto-label {
    font-weight: 700;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: #374151;
}

.wynik-content .punkt-text {
    font-size: 0.88rem;
    color: #1f2937;
    font-weight: 500;
    margin: 2px 0;
}

.wynik-content .uwagi-text {
    font-size: 0.81rem;
    color: #6b7a8d;
    line-height: 1.45;
}

/* ── Ocena końcowa ── */
.ocena-banner {
    border-radius: 10px;
    padding: 18px 24px;
    font-size: 1rem;
    font-weight: 600;
    text-align: center;
    margin-top: 20px;
    letter-spacing: 0.2px;
}

.ocena-ok      { background: #dcfce7; color: #14532d; border: 1px solid #86efac; }
.ocena-warn    { background: #fef9c3; color: #713f12; border: 1px solid #fde047; }
.ocena-blad    { background: #fee2e2; color: #7f1d1d; border: 1px solid #fca5a5; }

/* ── Sekcja upload ── */
.upload-section {
    background: #f8fafc;
    border: 2px dashed #cbd5e1;
    border-radius: 10px;
    padding: 16px;
    margin-bottom: 8px;
    transition: border-color 0.2s;
}

.upload-section:hover { border-color: #1b2d45; }

/* ── Separator sekcji ── */
.section-title {
    font-size: 0.75rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    color: #64748b;
    margin: 20px 0 10px;
    display: flex;
    align-items: center;
    gap: 8px;
}

.section-title::after {
    content: '';
    flex: 1;
    height: 1px;
    background: #e2e8f0;
}

/* ── Ukryj hamburger i footer ── */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# NAGŁÓWEK ABACUS
# =============================================================================

st.markdown("""
<div class="abacus-header">
    <div class="abacus-logo">📊</div>
    <div class="abacus-title-block">
        <h1>Kontrola Jakości – Zamknięcie Roku Obrachunkowego</h1>
        <p>Abacus Centrum Księgowe Puławy &nbsp;·&nbsp; Weryfikacja ZOiS · Bilans · Wyciąg Bankowy</p>
    </div>
</div>
""", unsafe_allow_html=True)


# =============================================================================
# SIDEBAR – DANE PODMIOTU I WGRYWANIE PLIKÓW
# =============================================================================

with st.sidebar:
    st.markdown("### 🏢 Podmiot")

    nazwa_podmiotu = st.text_input(
        "Nazwa podmiotu",
        placeholder="np. FIRMA SP. Z O.O.",
        help="Nazwa klienta – pojawi się w raporcie."
    )

    rok = st.selectbox(
        "Rok obrachunkowy",
        options=[2024, 2023, 2022],
        index=0,
    )

    st.markdown("---")
    st.markdown("### 📂 Pliki źródłowe")

    # ── ZOiS ────────────────────────────────────────────────────────────────
    st.markdown("**Zestawienie Obrotów i Sald**")
    plik_zois = st.file_uploader(
        "Wgraj ZOiS (XLSX lub PDF)",
        type=["xlsx", "xls", "pdf"],
        key="zois",
        label_visibility="collapsed",
    )
    if plik_zois:
        st.caption(f"✔ {plik_zois.name}")

    st.markdown("---")

    # ── Bilans ───────────────────────────────────────────────────────────────
    st.markdown("**Bilans**")
    plik_bilans = st.file_uploader(
        "Wgraj Bilans (XLSX lub PDF)",
        type=["xlsx", "xls", "pdf"],
        key="bilans",
        label_visibility="collapsed",
    )
    if plik_bilans:
        st.caption(f"✔ {plik_bilans.name}")

    st.markdown("---")

    # ── Wyciąg bankowy ───────────────────────────────────────────────────────
    st.markdown("**Wyciąg Bankowy (XII)**")
    plik_bank = st.file_uploader(
        "Wgraj Wyciąg (XLSX lub PDF)",
        type=["xlsx", "xls", "pdf"],
        key="bank",
        label_visibility="collapsed",
    )
    if plik_bank:
        st.caption(f"✔ {plik_bank.name}")

    # Alternatywnie – ręczne saldo bankowe
    saldo_reczne_str = st.text_input(
        "lub wpisz saldo bankowe ręcznie (zł)",
        placeholder="np. 125 000,00",
        help="Jeśli nie wgrywasz wyciągu – wpisz saldo końcowe z XII.",
    )

    st.markdown("---")

    # ── Opcje zaawansowane ────────────────────────────────────────────────────
    with st.expander("⚙️ Opcje zaawansowane"):
        tryb_testowy = st.checkbox(
            "Tryb testowy (dane syntetyczne)",
            value=False,
            help="Uruchom audyt na przykładowych danych bez wgrywania plików."
        )

    st.markdown("---")

    # ── Przycisk START ────────────────────────────────────────────────────────
    btn_audyt = st.button(
        "🔍 Uruchom Audyt",
        type="primary",
        use_container_width=True,
        disabled=(
            not tryb_testowy
            and plik_zois is None
        ),
    )

    if not tryb_testowy and plik_zois is None:
        st.caption("⬆ Wgraj przynajmniej plik ZOiS aby uruchomić audyt.")


# =============================================================================
# FUNKCJE POMOCNICZE
# =============================================================================

def pobierz_format(plik) -> str:
    """Wykrywa format pliku na podstawie rozszerzenia."""
    if plik is None:
        return "xlsx"
    nazwa = plik.name.lower()
    if nazwa.endswith(".pdf"):
        return "pdf"
    return "xlsx"


def renderuj_wynik(wynik: dict):
    """Renderuje pojedynczy wynik kontroli jako kolorowy wiersz HTML."""
    status_raw = wynik["status"]

    # Mapowanie statusu → klasa CSS i ikona
    mapa = {
        "✅ OK":            ("ok",      "✅"),
        "❌ BŁĄD":          ("blad",    "❌"),
        "⚠️  OSTRZEŻENIE":  ("ostrzez", "⚠️"),
        "🔍 BRAK DANYCH":   ("brak",    "🔍"),
        "ℹ️  INFO":          ("info",    "ℹ️"),
    }
    klasa, ikona = mapa.get(status_raw, ("info", "ℹ️"))

    wartosc_html = (
        f'<span style="font-size:0.78rem;color:#4b5563;margin-left:6px;">'
        f'[{wynik["wartosc"]}]</span>'
        if wynik.get("wartosc") else ""
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


def dane_testowe() -> tuple:
    """Zwraca syntetyczne dane testowe (bez plików)."""
    zois = DaneZOiS()
    zois.konta = {
        "130":  (Decimal("125000.00"), Decimal("0")),
        "145":  (Decimal("500.00"),    Decimal("0")),
        "200":  (Decimal("0"),         Decimal("3200.00")),
        "202":  (Decimal("0"),         Decimal("8750.00")),
        "229":  (Decimal("0"),         Decimal("4200.00")),
        "220":  (Decimal("0"),         Decimal("1890.00")),
        "230":  (Decimal("0"),         Decimal("12500.00")),
        "400":  (Decimal("45000.00"),  Decimal("0")),
        "401":  (Decimal("8200.00"),   Decimal("100.00")),
        "700":  (Decimal("0"),         Decimal("210000.00")),
    }
    zois.opisy = {
        "130": "Rachunek bankowy", "145": "Środki w drodze",
        "200": "Rozrachunki z odbiorcami", "202": "Rozrachunki z dostawcami",
        "400": "Zużycie materiałów", "401": "Wynagrodzenia",
        "700": "Sprzedaż usług",
    }
    bilans = DaneBilansu(
        aktywa_biezacy=Decimal("350000.00"),
        pasywa_biezacy=Decimal("350000.00"),
        aktywa_ubiegly=Decimal("280000.00"),
        pasywa_ubiegly=Decimal("280050.00"),
    )
    saldo_bank = Decimal("125000.00")
    return zois, bilans, saldo_bank


# =============================================================================
# GŁÓWNA LOGIKA – URUCHOMIENIE AUDYTU
# =============================================================================

if btn_audyt:
    if not nazwa_podmiotu.strip():
        nazwa_podmiotu = "Podmiot (nieznany)"

    audytor = SymfoniaYearEndAuditor()

    with st.spinner("⏳ Trwa weryfikacja danych..."):
        try:
            # ── Tryb testowy ─────────────────────────────────────────────────
            if tryb_testowy:
                dane_zois, dane_bilans, saldo_bank = dane_testowe()
                wyniki = audytor.check_accounting_logic(
                    dane_zois, dane_bilans, saldo_bank
                )
                raport = audytor.generate_audit_report(
                    wyniki,
                    nazwa_podmiotu=nazwa_podmiotu or "DEMO SP. Z O.O.",
                    rok=rok,
                )
                st.info("ℹ️ Uruchomiono w trybie testowym – dane syntetyczne.")

            # ── Tryb produkcyjny (prawdziwe pliki) ───────────────────────────
            else:
                # Parsowanie salda ręcznego
                saldo_reczne = None
                if saldo_reczne_str.strip():
                    try:
                        saldo_reczne = normalize_currency(saldo_reczne_str)
                    except ValueError:
                        st.warning(
                            "⚠️ Nie można odczytać ręcznie wpisanego salda bankowego. "
                            "Upewnij się, że używasz formatu: 125 000,00"
                        )

                raport = audytor.run_full_audit(
                    zois_bytes=plik_zois.read() if plik_zois else None,
                    zois_format=pobierz_format(plik_zois),

                    bilans_bytes=plik_bilans.read() if plik_bilans else None,
                    bilans_format=pobierz_format(plik_bilans),

                    bank_bytes=plik_bank.read() if plik_bank else None,
                    bank_format=pobierz_format(plik_bank),

                    saldo_bankowe_reczne=saldo_reczne,

                    nazwa_podmiotu=nazwa_podmiotu,
                    rok=rok,
                )

            # Zapis do session_state – raport pozostaje po odświeżeniu
            st.session_state["raport"] = raport
            st.session_state["podmiot"] = nazwa_podmiotu
            st.session_state["rok"] = rok

        except Exception as e:
            st.error(f"❌ Błąd podczas audytu: {e}")
            st.stop()


# =============================================================================
# WYŚWIETLANIE RAPORTU
# =============================================================================

if "raport" in st.session_state:
    raport   = st.session_state["raport"]
    podmiot  = st.session_state.get("podmiot", "")
    rok_disp = st.session_state.get("rok", 2024)
    podsum   = raport["podsumowanie"]
    wyniki   = raport["wyniki"]

    # ── Metryki podsumowania ─────────────────────────────────────────────────
    st.markdown(
        f'<div class="section-title">Wyniki audytu · {podmiot} · {rok_disp}</div>',
        unsafe_allow_html=True
    )

    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.markdown(f"""
        <div class="metric-card metric-ok">
            <div class="metric-value">{podsum['ok']}</div>
            <div class="metric-label">✅ Poprawnych</div>
        </div>""", unsafe_allow_html=True)

    with k2:
        st.markdown(f"""
        <div class="metric-card metric-blad">
            <div class="metric-value">{podsum['bledy']}</div>
            <div class="metric-label">❌ Błędów</div>
        </div>""", unsafe_allow_html=True)

    with k3:
        st.markdown(f"""
        <div class="metric-card metric-warn">
            <div class="metric-value">{podsum['ostrzezenia']}</div>
            <div class="metric-label">⚠️ Ostrzeżeń</div>
        </div>""", unsafe_allow_html=True)

    with k4:
        st.markdown(f"""
        <div class="metric-card metric-brak">
            <div class="metric-value">{podsum['brak_danych']}</div>
            <div class="metric-label">🔍 Brak danych</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Ocena końcowa ────────────────────────────────────────────────────────
    if podsum["bledy"] == 0 and podsum["ostrzezenia"] == 0:
        klasa_oceny = "ocena-ok"
        tekst_oceny = "🎉 DANE SPÓJNE – gotowe do badania sprawozdania finansowego."
    elif podsum["bledy"] == 0:
        klasa_oceny = "ocena-warn"
        tekst_oceny = "🟡 WYMAGA WYJAŚNIENIA – ostrzeżenia do weryfikacji z klientem."
    else:
        klasa_oceny = "ocena-blad"
        tekst_oceny = "🔴 DANE NIESPÓJNE – wymagane korekty przed zamknięciem roku."

    st.markdown(
        f'<div class="ocena-banner {klasa_oceny}">{tekst_oceny}</div>',
        unsafe_allow_html=True
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Tabela wyników ───────────────────────────────────────────────────────
    col_lewa, col_prawa = st.columns([2, 1])

    with col_lewa:
        st.markdown('<div class="section-title">Szczegółowe wyniki weryfikacji</div>',
                    unsafe_allow_html=True)

        # Filtry
        filtr_col1, filtr_col2 = st.columns(2)
        with filtr_col1:
            filtr_status = st.multiselect(
                "Filtruj status",
                options=["✅ OK", "❌ BŁĄD", "⚠️  OSTRZEŻENIE", "🔍 BRAK DANYCH", "ℹ️  INFO"],
                default=["❌ BŁĄD", "⚠️  OSTRZEŻENIE", "🔍 BRAK DANYCH"],
                label_visibility="collapsed",
            )
        with filtr_col2:
            pokaz_wszystkie = st.checkbox("Pokaż wszystkie wyniki", value=False)

        # Renderowanie wierszy
        wyniki_do_pokazania = wyniki if pokaz_wszystkie else [
            w for w in wyniki
            if pokaz_wszystkie or not filtr_status or w["status"] in filtr_status
        ]

        if not wyniki_do_pokazania:
            st.success("Brak wyników dla wybranych filtrów.")
        else:
            for wynik in wyniki_do_pokazania:
                renderuj_wynik(wynik)

    # ── Panel boczny raportu ────────────────────────────────────────────────
    with col_prawa:
        st.markdown('<div class="section-title">Eksport raportu</div>',
                    unsafe_allow_html=True)

        # Pobierz raport tekstowy
        tekst_raportu = raport["tekst"]
        st.download_button(
            label="⬇️ Pobierz raport TXT",
            data=tekst_raportu.encode("utf-8"),
            file_name=f"audyt_{podmiot.replace(' ', '_')}_{rok_disp}.txt",
            mime="text/plain",
            use_container_width=True,
        )

        # Eksport do CSV (tabela wyników)
        df_eksport = pd.DataFrame(wyniki)[
            ["konto", "status", "punkt", "uwagi", "wartosc"]
        ]
        df_eksport.columns = ["Konto", "Status", "Punkt kontroli", "Uwagi", "Wartość"]

        csv_bytes = df_eksport.to_csv(index=False, sep=";", encoding="utf-8-sig")
        st.download_button(
            label="⬇️ Pobierz wyniki CSV",
            data=csv_bytes.encode("utf-8"),
            file_name=f"audyt_{podmiot.replace(' ', '_')}_{rok_disp}.csv",
            mime="text/csv",
            use_container_width=True,
        )

        st.markdown("---")
        st.markdown('<div class="section-title">Statystyki</div>', unsafe_allow_html=True)

        # Mini tabela statystyk
        df_stat = pd.DataFrame([
            {"Status": "✅ OK",           "Liczba": podsum["ok"]},
            {"Status": "❌ Błędy",        "Liczba": podsum["bledy"]},
            {"Status": "⚠️ Ostrzeżenia",  "Liczba": podsum["ostrzezenia"]},
            {"Status": "🔍 Brak danych",  "Liczba": podsum["brak_danych"]},
        ])
        st.dataframe(df_stat, hide_index=True, use_container_width=True)

        # Wykres kołowy (jeśli są dane)
        import json
        total = podsum["ok"] + podsum["bledy"] + podsum["ostrzezenia"] + podsum["brak_danych"]
        if total > 0:
            try:
                import plotly.graph_objects as go
                fig = go.Figure(data=[go.Pie(
                    labels=["OK", "Błędy", "Ostrzeżenia", "Brak danych"],
                    values=[
                        podsum["ok"], podsum["bledy"],
                        podsum["ostrzezenia"], podsum["brak_danych"]
                    ],
                    marker_colors=["#22c55e", "#ef4444", "#f59e0b", "#94a3b8"],
                    hole=0.55,
                    textinfo="percent",
                    showlegend=False,
                )])
                fig.update_layout(
                    margin=dict(t=10, b=10, l=10, r=10),
                    height=200,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                pass  # plotly opcjonalne

        # Podgląd raportu tekstowego
        with st.expander("📄 Podgląd raportu tekstowego"):
            st.text(tekst_raportu)

# =============================================================================
# STAN POCZĄTKOWY (przed uruchomieniem audytu)
# =============================================================================

else:
    st.markdown('<div class="section-title">Jak korzystać z narzędzia</div>',
                unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    kroki = [
        ("1️⃣", "Wybierz podmiot", "Wpisz nazwę klienta i rok obrachunkowy w panelu bocznym."),
        ("2️⃣", "Wgraj pliki", "ZOiS, Bilans i Wyciąg Bankowy z Symfonii (XLSX lub PDF)."),
        ("3️⃣", "Uruchom audyt", "Kliknij przycisk 'Uruchom Audyt' – weryfikacja trwa kilka sekund."),
        ("4️⃣", "Pobierz raport", "Wyniki dostępne na ekranie + eksport TXT/CSV."),
    ]
    for col, (ikona, tytul, opis) in zip([c1, c2, c3, c4], kroki):
        with col:
            st.markdown(f"""
            <div class="metric-card" style="text-align:left; border-top: 4px solid #1b2d45;">
                <div style="font-size:1.6rem; margin-bottom:8px;">{ikona}</div>
                <div style="font-weight:700; font-size:0.88rem; color:#1b2d45; margin-bottom:4px;">{tytul}</div>
                <div style="font-size:0.79rem; color:#6b7a8d; line-height:1.4;">{opis}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.info(
        "💡 **Tryb testowy** – jeśli chcesz zobaczyć jak działa narzędzie bez wgrywania plików, "
        "zaznacz opcję **'Tryb testowy'** w panelu bocznym i kliknij 'Uruchom Audyt'.",
        icon="ℹ️"
    )

    # ── Tabela weryfikowanych reguł ──────────────────────────────────────────
    with st.expander("📋 Pełna lista weryfikowanych reguł"):
        reguly = pd.DataFrame([
            ("130", "Rachunek bankowy", "Saldo = Saldo końcowe wyciągu (XII)", "Muszą być równe"),
            ("145", "Środki w drodze", "Saldo końcowe = 0", "Zero"),
            ("200", "Rozrachunki z odbiorcami", "Saldo Ma > 0 = brak faktury sprzedaży", "Alert przy Ma"),
            ("202", "Rozrachunki z dostawcami", "Saldo Wn > 0 = brak faktury zakupu", "Alert przy Wn"),
            ("230", "Wynagrodzenia", "Saldo Wn = brak LP; Saldo Ma = niezapłacone", "Ma dopuszczalne"),
            ("229", "Rozrachunki ZUS", "Saldo Ma = zobowiązanie DRA", "Ma dopuszczalne"),
            ("220", "Rozrachunki US/PIT", "Saldo Ma = zobowiązanie PIT", "Ma dopuszczalne"),
            ("700", "Przychody ze sprzedaży", "Saldo Ma > 0; Saldo Wn = 0", "Weryfikacja przychodowości"),
            ("Gr. 4 (400–499)", "Koszty rodzajowe", "Wyłącznie Saldo Wn > 0", "Brak zapisów po stronie Ma"),
            ("Bilans", "Suma bilansowa", "Aktywa = Pasywa (rok bieżący i ubiegły)", "Musi się zgadzać"),
        ], columns=["Konto", "Nazwa", "Warunek weryfikacji", "Oczekiwany status"])

        st.dataframe(reguly, hide_index=True, use_container_width=True)
