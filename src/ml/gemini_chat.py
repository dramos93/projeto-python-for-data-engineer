import google.generativeai as genai
import os
from dotenv import load_dotenv
import polars as pl

load_dotenv()


class GeminiChat:
    def __init__(self):
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        self.model = genai.GenerativeModel("gemini-pro")
        self.chat = self.model.start_chat(history=[])

    def get_currency_context(
        self, df_heatmap: pl.DataFrame, df_volatility: pl.DataFrame
    ) -> str:
        """Cria contexto dos dados para o Gemini"""

        # Top 5 gainers e losers
        top_5_gain = df_heatmap.sort("daily_change_pct", descending=True).head(5)
        top_5_loss = df_heatmap.sort("daily_change_pct").head(5)

        # Moedas mais voláteis
        top_volatile = df_volatility.sort("volatility_30d", descending=True).head(5)

        context = f"""
Você é um assistente especializado em análise de câmbio. Aqui estão os dados atuais do mercado BRL:

TOP 5 MAIORES ALTAS:
{top_5_gain.select(["code_exchange_rate", "daily_change_pct"]).to_dicts()}

TOP 5 MAIORES QUEDAS:
{top_5_loss.select(["code_exchange_rate", "daily_change_pct"]).to_dicts()}

TOP 5 MAIS VOLÁTEIS (30 dias):
{top_volatile.select(["code_exchange_rate", "volatility_30d", "risk_score"]).to_dicts()}

Responda de forma objetiva e técnica, mas acessível. Use emoji quando apropriado.
"""
        return context

    def ask(self, question: str, context: str = "") -> str:
        """Envia pergunta para o Gemini"""
        try:
            prompt = f"{context}\n\nPergunta do usuário: {question}"
            response = self.chat.send_message(prompt)
            return response.text
        except Exception as e:
            return f"❌ Erro ao comunicar com Gemini: {str(e)}"

    def analyze_currency(self, currency: str, df_historical: pl.DataFrame) -> str:
        """Análise específica de uma moeda"""

        # Estatísticas básicas
        stats = df_historical.select(
            [
                pl.col("daily_change_pct").mean().alias("avg_change"),
                pl.col("daily_change_pct").std().alias("volatility"),
                pl.col("daily_change_pct").max().alias("max_gain"),
                pl.col("daily_change_pct").min().alias("max_loss"),
            ]
        ).to_dicts()[0]

        prompt = f"""
Analise a moeda {currency} baseado nos dados dos últimos 30 dias:

- Variação média diária: {stats["avg_change"]:.2f}%
- Volatilidade: {stats["volatility"]:.2f}%
- Maior alta: {stats["max_gain"]:.2f}%
- Maior queda: {stats["max_loss"]:.2f}%

Forneça:
1. Análise de tendência
2. Nível de risco
3. Recomendação (comprar/vender/aguardar)

Seja direto e objetivo (máximo 200 palavras).
"""

        response = self.model.generate_content(prompt)
        return response.text
