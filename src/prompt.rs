use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

fn unknown_string() -> String {
    "unknown".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Low,
    Medium,
    High,
    #[default]
    Unknown,
}

impl Severity {
    pub fn rank(&self) -> u8 {
        match self {
            Self::Unknown => 0,
            Self::Low => 1,
            Self::Medium => 2,
            Self::High => 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Finding {
    pub rule_id: String,
    #[serde(default)]
    pub severity: Severity,
    pub message: String,
    pub why_it_matters: String,
    #[serde(default, deserialize_with = "deserialize_evidence")]
    pub evidence: Vec<String>,
}

fn deserialize_evidence<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum EvidenceInput {
        Single(String),
        Many(Vec<String>),
    }

    let value = Option::<EvidenceInput>::deserialize(deserializer)?;
    Ok(match value {
        Some(EvidenceInput::Single(s)) => {
            if s.trim().is_empty() || s == "unknown" {
                Vec::new()
            } else {
                vec![s]
            }
        }
        Some(EvidenceInput::Many(v)) => v,
        None => Vec::new(),
    })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SqlExplanation {
    pub summary: String,
    #[serde(default)]
    pub tables: Vec<String>,
    #[serde(default)]
    pub joins: Vec<String>,
    #[serde(default)]
    pub filters: Vec<String>,
    #[serde(default)]
    pub risks: Vec<String>,
    #[serde(default)]
    pub suggestions: Vec<String>,
    #[serde(default)]
    pub anti_patterns: Vec<String>,
    #[serde(default)]
    pub findings: Vec<Finding>,
    #[serde(default = "unknown_string")]
    pub estimated_cost_impact: String,
    #[serde(default = "unknown_string")]
    pub confidence: String,
}

pub fn build_prompt(sql: &str) -> String {
    format!(
        r#"You are a SQL reviewer. ONLY use the SQL text provided. If something is unknown, write "unknown".
Return STRICT JSON with keys:
summary (string), tables (array), joins (array), filters (array), risks (array), suggestions (array),
anti_patterns (array), findings (array of objects with rule_id, severity, message, why_it_matters, evidence),
estimated_cost_impact (string: low|medium|high|unknown), confidence (string: low|medium|high|unknown).

SQL:
```sql
{sql}
```
"#
    )
}

pub fn parse_sql_explanation(raw_json: &str) -> anyhow::Result<SqlExplanation> {
    for candidate in parse_candidates(raw_json) {
        if let Ok(parsed) = serde_json::from_str::<SqlExplanation>(&candidate) {
            return Ok(parsed);
        }
    }

    let parse_err = serde_json::from_str::<SqlExplanation>(raw_json)
        .err()
        .map(|e| e.to_string())
        .unwrap_or_else(|| "unknown parse error".to_string());

    Err(anyhow::anyhow!(
        "Model did not return valid JSON. Try --json to inspect. Error: {parse_err}\nRaw:\n{raw_json}"
    ))
}

fn parse_candidates(raw: &str) -> Vec<String> {
    let mut out = vec![raw.trim().to_string()];

    let trimmed = raw.trim();
    if trimmed.starts_with("```") {
        let mut lines = trimmed.lines();
        let _opening = lines.next();
        let mut body = Vec::new();
        for line in lines {
            if line.trim_start().starts_with("```") {
                break;
            }
            body.push(line);
        }
        if !body.is_empty() {
            out.push(body.join("\n").trim().to_string());
        }
    }

    if let Some(json_slice) = first_json_object_slice(trimmed) {
        out.push(json_slice.trim().to_string());
    }

    out
}

fn first_json_object_slice(input: &str) -> Option<&str> {
    let bytes = input.as_bytes();
    let mut start = None;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, &b) in bytes.iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match b {
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match b {
            b'"' => in_string = true,
            b'{' => {
                if depth == 0 {
                    start = Some(idx);
                }
                depth += 1;
            }
            b'}' => {
                if depth > 0 {
                    depth -= 1;
                    if depth == 0 {
                        if let Some(s) = start {
                            return input.get(s..=idx);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{build_prompt, parse_sql_explanation, Severity};

    #[test]
    fn build_prompt_includes_sql_and_contract() {
        let sql = "select * from orders";
        let prompt = build_prompt(sql);

        assert!(prompt.contains("Return STRICT JSON with keys:"));
        assert!(prompt.contains("summary (string)"));
        assert!(prompt.contains("```sql"));
        assert!(prompt.contains(sql));
    }

    #[test]
    fn parse_sql_explanation_accepts_valid_json() {
        let raw = r#"{
            "summary":"Finds recent orders",
            "tables":["orders"],
            "joins":[],
            "filters":["created_at >= current_date - interval '30 days'"],
            "risks":["select * may read unnecessary columns"],
            "suggestions":["Project only needed columns"],
            "anti_patterns":["SELECT *"],
            "findings":[
                {
                    "rule_id":"SELECT_STAR",
                    "severity":"high",
                    "message":"SELECT *",
                    "why_it_matters":"Select star can scan unnecessary columns",
                    "evidence":["SELECT *"]
                }
            ],
            "estimated_cost_impact":"medium",
            "confidence":"high"
        }"#;

        let parsed = parse_sql_explanation(raw).expect("valid JSON should parse");
        assert_eq!(parsed.summary, "Finds recent orders");
        assert_eq!(parsed.tables, vec!["orders"]);
        assert_eq!(parsed.suggestions, vec!["Project only needed columns"]);
        assert_eq!(parsed.anti_patterns, vec!["SELECT *"]);
        assert_eq!(parsed.findings.len(), 1);
        assert_eq!(parsed.findings[0].rule_id, "SELECT_STAR");
        assert_eq!(parsed.findings[0].severity, Severity::High);
        assert_eq!(parsed.estimated_cost_impact, "medium");
        assert_eq!(parsed.confidence, "high");
    }

    #[test]
    fn parse_sql_explanation_defaults_new_fields_for_older_payloads() {
        let raw = r#"{
            "summary":"Legacy payload",
            "tables":[],
            "joins":[],
            "filters":[],
            "risks":[],
            "suggestions":[]
        }"#;

        let parsed = parse_sql_explanation(raw).expect("legacy JSON should still parse");
        assert!(parsed.anti_patterns.is_empty());
        assert!(parsed.findings.is_empty());
        assert_eq!(parsed.estimated_cost_impact, "unknown");
        assert_eq!(parsed.confidence, "unknown");
    }

    #[test]
    fn parse_sql_explanation_rejects_invalid_json() {
        let err = parse_sql_explanation("not json").expect_err("invalid JSON should fail");
        assert!(err.to_string().contains("Model did not return valid JSON"));
    }

    #[test]
    fn parse_sql_explanation_accepts_fenced_sql_json() {
        let raw = r#"```sql
{
  "summary":"ok",
  "tables":[],
  "joins":[],
  "filters":[],
  "risks":[],
  "suggestions":[]
}
```"#;

        let parsed = parse_sql_explanation(raw).expect("fenced JSON should parse");
        assert_eq!(parsed.summary, "ok");
    }

    #[test]
    fn parse_sql_explanation_accepts_preamble_plus_json() {
        let raw = r#"Here is the result:
{
  "summary":"ok",
  "tables":[],
  "joins":[],
  "filters":[],
  "risks":[],
  "suggestions":[]
}"#;

        let parsed = parse_sql_explanation(raw).expect("JSON with preamble should parse");
        assert_eq!(parsed.summary, "ok");
    }

    #[test]
    fn parse_sql_explanation_accepts_string_evidence() {
        let raw = r#"{
  "summary":"ok",
  "tables":[],
  "joins":[],
  "filters":[],
  "risks":[],
  "suggestions":[],
  "findings":[
    {
      "rule_id":"unknown",
      "severity":"unknown",
      "message":"has subquery",
      "why_it_matters":"unknown",
      "evidence":"unknown"
    }
  ]
}"#;

        let parsed = parse_sql_explanation(raw).expect("string evidence should parse");
        assert_eq!(parsed.findings.len(), 1);
        assert!(parsed.findings[0].evidence.is_empty());
    }
}
