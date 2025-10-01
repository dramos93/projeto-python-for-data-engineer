import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from src.ml.data_loader import DataLoader
from src.ml.gemini_chat import GeminiChat
from src.ml.ml_model import CurrencyPredictor
import polars as pl

# ============================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================
st.set_page_config(
    page_title="Currency Dashboard Pro",
    page_icon="💱",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================
# CACHE DE DADOS
# ============================================
@st.cache_data
def load_data():
    loader = DataLoader()
    return {
        "heatmap": loader.load_heatmap(),
        "gainers": loader.load_top_gainers(),
        "losers": loader.load_top_losers(),
        "volatility": loader.load_volatility(),
    }


@st.cache_resource
def init_gemini():
    return GeminiChat()


# ============================================
# CARREGAR DADOS
# ============================================
data = load_data()
gemini = init_gemini()

# ============================================
# SIDEBAR
# ============================================
with st.sidebar:
    st.title("💱 Currency Dashboard")
    st.markdown("---")

    page = st.radio(
        "Navegação", ["📊 Overview", "🔥 Heat Map", "📈 Previsão ML", "💬 Chat IA"]
    )

    st.markdown("---")
    st.markdown("### Filtros")

    # Filtro de risco
    risk_filter = st.multiselect(
        "Nível de Risco",
        ["🟢 Baixo", "🟡 Médio", "🔴 Alto"],
        default=["🟢 Baixo", "🟡 Médio", "🔴 Alto"],
    )

# ============================================
# PÁGINA: OVERVIEW
# ============================================
if page == "📊 Overview":
    st.title("📊 Dashboard de Câmbio BRL")

    # KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_currencies = len(data["heatmap"])
        st.metric("Moedas Monitoradas", total_currencies)

    with col2:
        avg_change = data["heatmap"]["daily_change_pct"].mean()
        st.metric("Variação Média", f"{avg_change:.2f}%")

    with col3:
        max_gain = data["heatmap"]["daily_change_pct"].max()
        st.metric("Maior Alta", f"{max_gain:.2f}%", delta=f"{max_gain:.2f}%")

    with col4:
        max_loss = data["heatmap"]["daily_change_pct"].min()
        st.metric(
            "Maior Queda",
            f"{max_loss:.2f}%",
            delta=f"{max_loss:.2f}%",
            delta_color="inverse",
        )

    st.markdown("---")

    # Top Movers
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🔥 Top 10 Gainers")
        st.dataframe(
            data["gainers"].to_pandas(), width='stretch', hide_index=True
        )

    with col2:
        st.subheader("❄️ Top 10 Losers")
        st.dataframe(
            data["losers"].to_pandas(), width='stretch', hide_index=True
        )

# ============================================
# PÁGINA: HEAT MAP
# ============================================
elif page == "🔥 Heat Map":
    st.title("🔥 Heat Map de Performance")

    # Filtrar por risco
    df_filtered = data["heatmap"]
    # .filter(pl.col("performance_tier").is_in(risk_filter))

    # Gráfico de treemap
    fig = px.treemap(
        df_filtered.to_pandas(),
        path=[px.Constant("BRL"), "code_exchange_rate"],
        values="exchange_rate",
        color="daily_change_pct",
        color_continuous_scale=["red", "yellow", "green"],
        color_continuous_midpoint=0,
        title="Heat Map de Cotações BRL",
    )

    fig.update_layout(height=600)
    st.plotly_chart(fig, width='stretch')

    st.markdown("---")

    # Tabela de volatilidade
    st.subheader("📊 Análise de Volatilidade")

    df_vol_filtered = data["volatility"].filter(pl.col("risk_score").is_in(risk_filter))

    st.dataframe(df_vol_filtered.to_pandas(), width='stretch', hide_index=True)

# ============================================
# PÁGINA: PREVISÃO ML
# ============================================
elif page == "📈 Previsão ML":
    st.title("📈 Previsão com Machine Learning")

    # Seletor de moeda
    currencies = data["heatmap"]["code_exchange_rate"].to_list()
    selected_currency = st.selectbox("Selecione a moeda", currencies)

    if st.button("🚀 Gerar Previsão"):
        with st.spinner("Treinando modelo Prophet..."):
            try:
                # Carregar histórico
                loader = DataLoader()
                df_historical = loader.load_historical(selected_currency)

                # Treinar modelo
                predictor = CurrencyPredictor()
                predictor.train(df_historical, selected_currency)

                # Fazer previsão
                df_forecast = predictor.predict(periods=7)

                # Visualizar
                fig = go.Figure()

                # Histórico
                fig.add_trace(
                    go.Scatter(
                        x=df_historical["time_last_update_utc"].to_list(),
                        y=df_historical["exchange_rate"].to_list(),
                        mode="lines",
                        name="Histórico",
                        line=dict(color="blue"),
                    )
                )

                # Previsão
                fig.add_trace(
                    go.Scatter(
                        x=df_forecast["ds"].to_list(),
                        y=df_forecast["yhat"].to_list(),
                        mode="lines+markers",
                        name="Previsão",
                        line=dict(color="red", dash="dash"),
                    )
                )

                # Intervalo de confiança
                fig.add_trace(
                    go.Scatter(
                        x=df_forecast["ds"].to_list(),
                        y=df_forecast["yhat_upper"].to_list(),
                        fill=None,
                        mode="lines",
                        line=dict(color="lightgray"),
                        showlegend=False,
                    )
                )

                fig.add_trace(
                    go.Scatter(
                        x=df_forecast["ds"].to_list(),
                        y=df_forecast["yhat_lower"].to_list(),
                        fill="tonexty",
                        mode="lines",
                        line=dict(color="lightgray"),
                        name="Intervalo 95%",
                    )
                )

                fig.update_layout(
                    title=f"Previsão para {selected_currency}",
                    xaxis_title="Data",
                    yaxis_title="Taxa de Câmbio (BRL)",
                    height=500,
                )

                st.plotly_chart(fig, width='stretch')

                # Tabela de previsões
                st.subheader("📋 Valores Previstos")
                st.dataframe(df_forecast.to_pandas(), width='stretch')

            except Exception as e:
                st.error(f"❌ Erro ao gerar previsão: {str(e)}")

# ============================================
# PÁGINA: CHAT IA
# ============================================
elif page == "💬 Chat IA":
    st.title("💬 Chat com IA (Gemini)")

    # Inicializar histórico de chat
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Mostrar histórico
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Input do usuário
    if prompt := st.chat_input("Pergunte sobre o mercado de câmbio..."):
        # Adicionar mensagem do usuário
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("user"):
            st.markdown(prompt)

        # Gerar resposta
        with st.chat_message("assistant"):
            with st.spinner("Pensando..."):
                # Contexto dos dados
                context = gemini.get_currency_context(
                    data["heatmap"], data["volatility"]
                )

                response = gemini.ask(prompt, context)
                st.markdown(response)

        # Adicionar resposta ao histórico
        st.session_state.messages.append({"role": "assistant", "content": response})

    # Botões de sugestão
    st.markdown("---")
    st.markdown("### 💡 Sugestões de Perguntas")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Quais moedas estão mais voláteis?"):
            st.rerun()

    with col2:
        if st.button("O que está impactando o mercado hoje?"):
            st.rerun()

    with col3:
        if st.button("Qual moeda é mais segura agora?"):
            st.rerun()
