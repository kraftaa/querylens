# Why Build `querylens` If Anyone Can Just Ask Claude or ChatGPT?

Great question.

If anyone can paste SQL into an LLM and get an explanation, why build a tool?

Because a **chatbot reply is not a software product**.

---

## 1️⃣ LLMs Are Assistants — Not Tools

General chatbots:
- Produce free-text responses
- Have no output contract
- Are inconsistent across sessions
- Can hallucinate
- Cannot integrate into workflows

A real tool provides:
- Structured JSON output
- Deterministic formatting
- Versioned behavior
- Programmatic integration
- CLI/API automation

That’s the difference between *chatting* and *engineering*.

---

## 2️⃣ Domain Context Makes It Better

A generic LLM:
- Doesn’t know your schema
- Doesn’t know your company style rules
- Doesn’t know performance expectations
- Doesn’t know historical query behavior

`querylens` can integrate:
- Schema metadata
- Static analysis rules
- Query linting
- Explain plan insights
- Organizational standards

This makes it reliable — not just plausible.

---

## 3️⃣ Add Static Analysis + Guardrails

Before calling an LLM, a tool can:

- Detect `SELECT *`
- Detect missing `LIMIT`
- Detect leading wildcard `LIKE '%x'`
- Detect joins without conditions
- Flag potential full table scans

Then combine that with model reasoning.

Now you’re not relying purely on AI.
You’re building an AI-augmented system.

---

## 4️⃣ Automation & CI Integration

You can:

- Run in CI to block risky SQL
- Integrate into VS Code
- Provide API endpoints
- Attach to PR reviews
- Enforce governance policies

You cannot automate a chatbot session.

---

## 5️⃣ Structured Output Matters

Instead of:

> “This query joins two tables and might be slow…”

You get:

```json
{
  "summary": "...",
  "tables": ["orders", "customers"],
  "joins": ["INNER JOIN on customer_id"],
  "risks": ["Missing index on customer_id"],
  "suggestions": ["Add index on orders.customer_id"]
}

How to run a real smoke test locally:

export OPENAI_API_KEY="..."
export OPENAI_MODEL="gpt-4.1-mini"
./scripts/smoke-test.sh openai
export AWS_REGION="us-east-1"
export BEDROCK_MODEL_ID="anthropic.claude-3-5-sonnet-20241022-v2:0"
./scripts/smoke-test.sh bedrock