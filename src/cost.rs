use crate::insights::extract_tables;
use crate::prompt::SqlExplanation;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableStats {
    pub table_name: Option<String>,
    pub total_bytes: Option<u64>,
    pub row_count: Option<u64>,
    pub partition_columns: Vec<String>,
    pub column_count: Option<u32>,
    pub format: Option<String>, // parquet, csv, iceberg, orc, etc.
}

pub type StatsMap = HashMap<String, TableStats>;

#[derive(Debug, Serialize)]
pub struct CostEstimate {
    pub file: String,
    pub engine: String,
    pub estimated_scan_bytes: Option<u64>,
    pub estimated_scan_human: String,
    pub estimated_cost_usd: Option<f64>,
    pub risk: String,
    pub confidence: String,
    pub signals: Vec<String>,
}

pub fn load_stats_map(path: &std::path::Path) -> anyhow::Result<StatsMap> {
    let raw = std::fs::read_to_string(path)?;
    let value: serde_json::Value = serde_json::from_str(&raw)?;
    if let Some(map) = value.get("tables").and_then(|v| v.as_object()) {
        let mut out = StatsMap::new();
        for (name, node) in map {
            let stats: TableStats = serde_json::from_value(node.clone())?;
            out.insert(name.clone(), stats);
        }
        return Ok(out);
    }
    // fallback: flat map
    let parsed: StatsMap = serde_json::from_value(value)?;
    Ok(parsed)
}

pub fn estimate_cost(
    sql: &str,
    parsed: &SqlExplanation,
    engine: &str,
    stats_map: &StatsMap,
    scan_bytes_override: Option<u64>,
    scan_tb_override: Option<f64>,
) -> CostEstimate {
    // If explicit overrides are provided, use them directly.
    if let Some(bytes) = scan_bytes_override {
        return make_estimate(sql, engine, Some(bytes), parsed, Vec::new());
    }
    if let Some(tb) = scan_tb_override {
        let bytes = (tb * 1_000_000_000_000_f64) as u64;
        return make_estimate(sql, engine, Some(bytes), parsed, Vec::new());
    }

    let mut total: u64 = 0;
    let mut signals = Vec::new();

    let tables = extract_tables(sql);
    for table in tables {
        if let Some(stats) = stats_map.get(&table) {
            if let Some(bytes) = stats.total_bytes {
                let partition_factor = estimate_partition_factor(&stats.partition_columns, parsed);
                let column_factor = estimate_column_factor(
                    None,
                    stats.column_count,
                    stats.format.as_deref(),
                    parsed.findings.iter().any(|f| f.rule_id == "SELECT_STAR"),
                );
                let estimated = (bytes as f64 * partition_factor * column_factor) as u64;
                total = total.saturating_add(estimated);
            } else {
                signals.push(format!("{table}: missing total_bytes in stats"));
            }
        } else {
            signals.push(format!("{table}: no stats found"));
        }
    }

    if total == 0 {
        return make_estimate(sql, engine, None, parsed, signals);
    }

    make_estimate(sql, engine, Some(total), parsed, signals)
}

fn estimate_partition_factor(partition_columns: &[String], parsed: &SqlExplanation) -> f64 {
    if partition_columns.is_empty() {
        return 1.0;
    }
    let filters = parsed
        .findings
        .iter()
        .filter_map(|f| {
            if f.rule_id == "ATHENA_MISSING_PARTITION_FILTER" {
                Some("missing_partition".to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if filters.is_empty() {
        return 0.25; // assume some pruning
    }
    1.0
}

fn estimate_column_factor(
    selected_columns: Option<usize>,
    total_columns: Option<u32>,
    format: Option<&str>,
    select_star: bool,
) -> f64 {
    if select_star {
        return 1.0;
    }
    let is_columnar = matches!(format, Some("parquet") | Some("orc") | Some("iceberg"));
    if !is_columnar {
        return 1.0;
    }
    match (selected_columns, total_columns) {
        (Some(sel), Some(total)) if total > 0 => {
            let f = sel as f64 / total as f64;
            f.clamp(0.05, 1.0)
        }
        _ => 0.5,
    }
}

fn athena_cost_usd(bytes: u64) -> f64 {
    const BYTES_PER_TB: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;
    (bytes as f64 / BYTES_PER_TB) * 5.0
}

fn make_estimate(
    file: &str,
    engine: &str,
    bytes: Option<u64>,
    _parsed: &SqlExplanation,
    mut signals: Vec<String>,
) -> CostEstimate {
    let estimated_scan_human = bytes_to_human_label(bytes);
    let estimated_cost_usd = match (engine, bytes) {
        ("athena", Some(b)) => Some(athena_cost_usd(b)),
        _ => None,
    };
    if signals.is_empty() {
        signals.push("estimated from stats/heuristics".to_string());
    }

    CostEstimate {
        file: file.to_string(),
        engine: engine.to_string(),
        estimated_scan_bytes: bytes,
        estimated_scan_human,
        estimated_cost_usd,
        risk: "info".to_string(),
        confidence: if bytes.is_some() { "medium" } else { "low" }.to_string(),
        signals,
    }
}

fn bytes_to_human_label(bytes: Option<u64>) -> String {
    match bytes {
        Some(bytes) if bytes >= 1_000_000_000_000 => {
            format!("{:.2} TB", bytes as f64 / 1_000_000_000_000_f64)
        }
        Some(bytes) if bytes >= 1_000_000_000 => {
            format!("{:.0} GB", bytes as f64 / 1_000_000_000_f64)
        }
        Some(bytes) if bytes >= 1_000_000 => {
            format!("{:.0} MB", bytes as f64 / 1_000_000_f64)
        }
        Some(bytes) => format!("{bytes} B"),
        None => "unknown".to_string(),
    }
}

pub fn collect_postgres_stats(conn: &str) -> anyhow::Result<serde_json::Value> {
    let sql = r#"
SELECT
  relname AS table_name,
  pg_total_relation_size(oid) AS total_bytes,
  reltuples::bigint AS row_count
FROM pg_class
WHERE relkind = 'r'
ORDER BY relname;
"#;
    let output = std::process::Command::new("psql")
        .arg(conn)
        .arg("-t")
        .arg("-A")
        .arg("-F")
        .arg(",")
        .arg("-c")
        .arg(sql)
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("psql failed: {stderr}"));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut map = serde_json::Map::new();
    for line in stdout.lines() {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() != 3 {
            continue;
        }
        let name = parts[0].trim();
        let bytes = parts[1].trim().parse::<u64>().ok();
        let rows = parts[2].trim().parse::<u64>().ok();
        let stats = serde_json::json!({
            "table_name": name,
            "total_bytes": bytes,
            "row_count": rows,
        });
        map.insert(name.to_string(), stats);
    }
    Ok(serde_json::json!({ "tables": map }))
}
