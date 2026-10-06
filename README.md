# Bot-news

Telegram news bot with batch AI ranking, a per-day delivery limit, article
summaries, vote collection, and an adaptive recommendation profile.

## AI provider

AI requests use the OpenRouter OpenAI-compatible Chat Completions API. Create an
API key at [OpenRouter](https://openrouter.ai/keys), then set
`OPENROUTER_API_KEY` in the bot's environment file. Do not commit or share the
key. `OPENROUTER_MODEL` defaults to `google/gemini-2.5-flash-lite`, currently
priced at $0.10 per million input tokens and $0.40 per million output tokens
(check the [model page](https://openrouter.ai/google/gemini-2.5-flash-lite) for
current pricing). It returns JSON for the existing batch ranking and profile
updates.

Copy `.env.example` to your environment file and set at least `BOT_TOKEN`,
`CHAT_ID`, and `OPENROUTER_API_KEY`. Existing batch, daily limit, and profile
settings can be left at their documented defaults.
