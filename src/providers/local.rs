use crate::error::AppError;
use crate::providers::LlmProvider;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::json;

pub struct LocalProvider {
    client: reqwest::Client,
    base_url: String,
    model: String,
    api_key: Option<String>,
}

impl LocalProvider {
    pub fn new(base_url: String, model: String, api_key: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            model,
            api_key,
        }
    }
}

fn extract_local_text(v: &serde_json::Value) -> Result<String, AppError> {
    let content = &v["choices"][0]["message"]["content"];

    if let Some(text) = content.as_str() {
        return Ok(text.to_string());
    }

    if let Some(parts) = content.as_array() {
        let merged = parts
            .iter()
            .filter_map(|part| part["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");

        if !merged.is_empty() {
            return Ok(merged);
        }
    }

    Err(AppError::Provider(format!(
        "Unexpected local response shape: {v}"
    )))
}

#[async_trait::async_trait]
impl LlmProvider for LocalProvider {
    async fn explain_sql_json(&self, prompt: &str) -> Result<String, AppError> {
        let body = json!({
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.2,
            "response_format": {
                "type": "json_object"
            }
        });

        let url = format!("{}/v1/chat/completions", self.base_url);
        let mut request = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .json(&body);

        if let Some(api_key) = &self.api_key {
            request = request.header(AUTHORIZATION, format!("Bearer {api_key}"));
        }

        let resp = request
            .send()
            .await
            .map_err(|e| AppError::Provider(format!("Local LLM request failed: {e}")))?;

        let status = resp.status();
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| AppError::Provider(format!("Local LLM JSON parse failed: {e}")))?;

        if !status.is_success() {
            return Err(AppError::Provider(format!(
                "Local LLM error status={status}, body={v}"
            )));
        }

        extract_local_text(&v)
    }
}

#[cfg(test)]
mod tests {
    use super::extract_local_text;
    use serde_json::json;

    #[test]
    fn extracts_string_content() {
        let payload = json!({
            "choices": [
                {
                    "message": {
                        "content": r#"{\"summary\":\"ok\",\"tables\":[],\"joins\":[],\"filters\":[],\"risks\":[],\"suggestions\":[]}"#
                    }
                }
            ]
        });

        let text = extract_local_text(&payload).expect("should extract string content");
        assert!(text.contains("summary"));
    }

    #[test]
    fn extracts_array_content() {
        let payload = json!({
            "choices": [
                {
                    "message": {
                        "content": [
                            {
                                "type": "text",
                                "text": "{"
                            },
                            {
                                "type": "text",
                                "text": "\"summary\":\"ok\"}"
                            }
                        ]
                    }
                }
            ]
        });

        let text = extract_local_text(&payload).expect("should extract array content");
        assert!(text.contains("summary"));
    }

    #[test]
    fn rejects_unexpected_shape() {
        let payload = json!({ "foo": "bar" });
        let err = extract_local_text(&payload).expect_err("unexpected shape should fail");
        assert!(err.to_string().contains("Unexpected local response shape"));
    }
}
