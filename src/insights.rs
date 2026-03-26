use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageItem {
    pub output: String,
    pub expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageReport {
    pub projections: Vec<LineageItem>,
    pub filters: Vec<String>,
    pub joins: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryExplanation {
    pub purpose: String,
    pub tables: Vec<String>,
    pub joins: Vec<String>,
    pub aggregations: Vec<String>,
    pub aggregation_details: Vec<String>,
    pub meaning: String,
}

pub fn extract_tables(sql: &str) -> Vec<String> {
    let mut tables = Vec::new();

    // Capture dbt-style references before stripping Jinja.
    tables.extend(extract_dbt_refs(sql));

    // Strip Jinja/templating blocks so token scan ignores them.
    let normalized = strip_jinja(sql).to_ascii_lowercase();
    let tokens: Vec<&str> = normalized.split_whitespace().collect();

    let mut i = 0;
    while i + 1 < tokens.len() {
        let token = tokens[i];
        if token == "from" || token == "join" {
            let candidate =
                tokens[i + 1].trim_matches(|c: char| c == ',' || c == ';' || c == '(' || c == ')');
            if !candidate.is_empty()
                && candidate != "select"
                && candidate != "("
                && !tables.iter().any(|t| t == candidate)
            {
                tables.push(candidate.to_string());
            }
        }
        i += 1;
    }

    tables
}

fn strip_jinja(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' && matches!(chars.peek(), Some('{') | Some('%')) {
            // consume the opening
            chars.next();
            // skip until closing }} or %}.
            while let Some(n) = chars.next() {
                if (n == '}' && matches!(chars.peek(), Some('}')))
                    || (n == '%' && matches!(chars.peek(), Some('}')))
                {
                    chars.next();
                    break;
                }
            }
            out.push(' ');
            continue;
        }
        out.push(c);
    }
    out
}

fn extract_dbt_refs(sql: &str) -> Vec<String> {
    let lower = sql.to_ascii_lowercase();
    let mut refs = Vec::new();
    let mut idx = 0;
    while let Some(pos) = lower[idx..].find("ref(") {
        let start = idx + pos + 4;
        if let Some(end) = lower[start..].find(')') {
            let arg = lower[start..start + end].trim();
            if let Some(name) = trim_quotes(arg) {
                refs.push(name.to_string());
            }
            idx = start + end;
        } else {
            break;
        }
    }

    idx = 0;
    while let Some(pos) = lower[idx..].find("source(") {
        let start = idx + pos + 7;
        if let Some(end) = lower[start..].find(')') {
            let args = lower[start..start + end].split(',').collect::<Vec<_>>();
            if args.len() >= 2 {
                if let Some(name) = trim_quotes(args[1].trim()) {
                    refs.push(name.to_string());
                }
            }
            idx = start + end;
        } else {
            break;
        }
    }

    refs
}

fn trim_quotes(s: &str) -> Option<&str> {
    if (s.starts_with('\"') && s.ends_with('\"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        return Some(&s[1..s.len().saturating_sub(1)]);
    }
    None
}

pub fn extract_lineage(sql: &str) -> Vec<LineageItem> {
    extract_lineage_report(sql).projections
}

pub fn extract_lineage_report(sql: &str) -> LineageReport {
    let (ctes, outer_query) = extract_ctes_and_outer_query(sql);
    let target_query = resolve_star_select_target(&outer_query, &ctes, 0);

    let aliases = extract_aliases(&target_query);
    let projections = extract_projection_lineage(&target_query, &aliases);
    let filters = extract_filter_lineage(&target_query, &aliases);
    let joins = extract_join_lineage(&target_query, &aliases);

    LineageReport {
        projections,
        filters,
        joins,
    }
}

fn extract_projection_lineage(sql: &str, aliases: &HashMap<String, String>) -> Vec<LineageItem> {
    let lower = sql.to_ascii_lowercase();
    let Some(select_pos) = lower.find("select") else {
        return Vec::new();
    };
    let Some(from_pos) = lower[select_pos..].find("from").map(|p| p + select_pos) else {
        return Vec::new();
    };

    let select_clause = &sql[select_pos + 6..from_pos];
    let parts = split_top_level(select_clause, ',');
    let mut items = Vec::new();

    for part in parts {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }

        let (output, expression) = parse_select_item(trimmed);
        let expression = resolve_aliases(&expression, aliases);
        items.push(LineageItem { output, expression });
    }

    items
}

pub fn explain_query(sql: &str) -> QueryExplanation {
    let report = extract_lineage_report(sql);
    let tables = extract_tables(sql);
    let lower = sql.to_ascii_lowercase();
    let mut aggregations = Vec::new();

    for agg in ["sum(", "count(", "avg(", "min(", "max("] {
        if lower.contains(agg) {
            aggregations.push(agg.trim_end_matches('(').to_ascii_uppercase());
        }
    }

    let aggregation_details = report
        .projections
        .iter()
        .filter(|item| contains_aggregate(&item.expression))
        .map(|item| {
            if item.output == item.expression {
                item.expression.clone()
            } else {
                format!("{} AS {}", item.expression, item.output)
            }
        })
        .collect::<Vec<_>>();

    let meaning = build_meaning(&report.projections, &aggregation_details, &tables);
    let purpose = if !aggregations.is_empty() {
        "calculate aggregate metrics".to_string()
    } else if tables.is_empty() {
        "query data".to_string()
    } else {
        format!("read data from {}", tables.join(", "))
    };

    QueryExplanation {
        purpose,
        tables,
        joins: report.joins,
        aggregations,
        aggregation_details,
        meaning,
    }
}

fn contains_aggregate(expression: &str) -> bool {
    let lower = expression.to_ascii_lowercase();
    ["sum(", "count(", "avg(", "min(", "max("]
        .iter()
        .any(|agg| lower.contains(agg))
}

fn build_meaning(
    projections: &[LineageItem],
    aggregation_details: &[String],
    tables: &[String],
) -> String {
    if !aggregation_details.is_empty() {
        let metric = projections
            .iter()
            .find(|item| contains_aggregate(&item.expression))
            .map(|item| humanize_label(&item.output))
            .unwrap_or_else(|| "aggregated result".to_string());
        let dimensions = projections
            .iter()
            .filter(|item| !contains_aggregate(&item.expression))
            .map(describe_dimension)
            .filter(|label| !label.is_empty())
            .collect::<Vec<_>>();

        if dimensions.is_empty() {
            return format!("{metric} total");
        }

        return format!("{metric} per {}", join_labels(&dimensions));
    }

    if tables.is_empty() {
        "query result".to_string()
    } else {
        format!("rows from {}", tables.join(", "))
    }
}

fn describe_dimension(item: &LineageItem) -> String {
    let output = humanize_label(&item.output);
    if output == "id" || output == "email" || output == "date" || output == "created at" {
        return describe_from_expression(&item.expression).unwrap_or(output);
    }
    output
}

fn describe_from_expression(expression: &str) -> Option<String> {
    let token = expression
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
        .find(|part| part.contains('.'))?;
    let (table, column) = token.split_once('.')?;
    let entity = singularize_label(table);
    let column_label = humanize_column(column);

    if column_label == "id" {
        return Some(format!("{entity} id"));
    }
    if column_label == "email" {
        return Some(format!("{entity} email"));
    }
    if column_label == "date" || column_label == "created at" {
        return Some(column_label.to_string());
    }

    Some(format!("{entity} {column_label}"))
}

fn singularize_label(label: &str) -> String {
    let base = label.rsplit('.').next().unwrap_or(label);
    if let Some(stripped) = base.strip_suffix("ies") {
        return format!("{stripped}y");
    }
    if base.ends_with('s') && base.len() > 1 {
        return base[..base.len() - 1].to_string();
    }
    base.to_string()
}

fn humanize_column(label: &str) -> String {
    if label.ends_with("_at") || label.ends_with("_date") {
        return "date".to_string();
    }
    humanize_label(label)
}

fn humanize_label(label: &str) -> String {
    let base = label
        .rsplit('.')
        .next()
        .unwrap_or(label)
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('\'');
    let base = base.strip_suffix("_id").unwrap_or(base);
    base.replace('_', " ")
}

fn join_labels(labels: &[String]) -> String {
    match labels {
        [] => String::new(),
        [one] => one.clone(),
        [first, second] => format!("{first} and {second}"),
        _ => {
            let mut out = labels[..labels.len() - 1].join(", ");
            out.push_str(", and ");
            out.push_str(&labels[labels.len() - 1]);
            out
        }
    }
}

fn extract_aliases(sql: &str) -> HashMap<String, String> {
    let normalized = sql.to_ascii_lowercase();
    let tokens: Vec<&str> = normalized
        .split_whitespace()
        .map(|t| t.trim_matches(|c: char| matches!(c, ',' | ';' | '(' | ')')))
        .collect();

    let mut aliases = HashMap::new();
    let mut i = 0;
    while i + 1 < tokens.len() {
        if tokens[i] == "from" || tokens[i] == "join" {
            let table = tokens[i + 1].to_string();
            let mut alias: Option<String> = None;

            if i + 2 < tokens.len() && tokens[i + 2] == "as" && i + 3 < tokens.len() {
                alias = Some(tokens[i + 3].to_string());
            } else if i + 2 < tokens.len() {
                let candidate = tokens[i + 2];
                if !matches!(
                    candidate,
                    "on" | "using"
                        | "where"
                        | "join"
                        | "left"
                        | "right"
                        | "inner"
                        | "full"
                        | "cross"
                        | "group"
                        | "order"
                        | "limit"
                        | "having"
                ) {
                    alias = Some(candidate.to_string());
                }
            }

            if let Some(alias) = alias {
                aliases.insert(alias, table.clone());
            }
            aliases.insert(table.clone(), table);
        }
        i += 1;
    }

    aliases
}

fn resolve_aliases(input: &str, aliases: &HashMap<String, String>) -> String {
    let mut out = String::new();
    let mut token = String::new();

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' {
            token.push(ch);
        } else {
            if !token.is_empty() {
                out.push_str(&resolve_token(&token, aliases));
                token.clear();
            }
            out.push(ch);
        }
    }
    if !token.is_empty() {
        out.push_str(&resolve_token(&token, aliases));
    }

    out
}

fn resolve_token(token: &str, aliases: &HashMap<String, String>) -> String {
    if let Some((alias, rest)) = token.split_once('.') {
        if let Some(table) = aliases.get(alias) {
            return format!("{table}.{rest}");
        }
    }
    token.to_string()
}

fn extract_filter_lineage(sql: &str, aliases: &HashMap<String, String>) -> Vec<String> {
    let compact = normalize_whitespace(sql);
    let lower = compact.to_ascii_lowercase();
    let Some(where_start) = lower.find("where") else {
        return Vec::new();
    };
    let clause_start = where_start + "where".len();
    let rest = &lower[clause_start..];

    let mut end = lower.len();
    for kw in ["group by", "order by", "limit", "having"] {
        if let Some(pos) = rest.find(kw) {
            end = end.min(clause_start + pos);
        }
    }

    let raw = compact[clause_start..end].trim();
    if raw.is_empty() {
        Vec::new()
    } else {
        vec![resolve_aliases(raw, aliases)]
    }
}

fn extract_join_lineage(sql: &str, aliases: &HashMap<String, String>) -> Vec<String> {
    let compact = normalize_whitespace(sql);
    let lower = compact.to_ascii_lowercase();
    let mut joins = Vec::new();
    let mut search_from = 0usize;

    while let Some(join_pos_rel) = lower[search_from..].find("join ") {
        let join_pos = search_from + join_pos_rel;
        let segment = &lower[join_pos..];

        if let Some(on_rel) = segment.find(" on ") {
            let on_start = join_pos + on_rel + 4;
            let rest = &lower[on_start..];
            let mut end = lower.len();
            for kw in [
                " join ",
                " where ",
                " group by ",
                " order by ",
                " limit ",
                " having ",
            ] {
                if let Some(pos) = rest.find(kw) {
                    end = end.min(on_start + pos);
                }
            }
            let raw = compact[on_start..end].trim();
            if !raw.is_empty() {
                joins.push(resolve_aliases(raw, aliases));
            }
            search_from = on_start;
            continue;
        }

        if let Some(using_rel) = segment.find(" using ") {
            let using_start = join_pos + using_rel + 7;
            let rest = &lower[using_start..];
            let mut end = lower.len();
            for kw in [
                " join ",
                " where ",
                " group by ",
                " order by ",
                " limit ",
                " having ",
            ] {
                if let Some(pos) = rest.find(kw) {
                    end = end.min(using_start + pos);
                }
            }
            let raw = compact[using_start..end].trim();
            if !raw.is_empty() {
                joins.push(raw.to_string());
            }
            search_from = using_start;
            continue;
        }

        search_from = join_pos + 6;
    }

    joins
}

fn normalize_whitespace(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn extract_ctes_and_outer_query(sql: &str) -> (HashMap<String, String>, String) {
    let prepared = strip_leading_template_blocks(sql);
    let compact = normalize_whitespace(&prepared);
    let lower = compact.to_ascii_lowercase();
    if !lower.starts_with("with ") {
        return (HashMap::new(), compact);
    }

    let mut idx = 5usize; // after "with "
    let mut ctes = HashMap::new();
    let bytes = compact.as_bytes();
    let len = compact.len();

    loop {
        while idx < len && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= len {
            break;
        }

        let name_start = idx;
        while idx < len
            && (bytes[idx].is_ascii_alphanumeric() || bytes[idx] == b'_' || bytes[idx] == b'.')
        {
            idx += 1;
        }
        if idx == name_start {
            break;
        }
        let cte_name = compact[name_start..idx].to_ascii_lowercase();

        while idx < len && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }

        // Optional column list: cte_name(col1, col2)
        if idx < len && bytes[idx] == b'(' {
            if let Some(end) = find_matching_paren(&compact, idx) {
                idx = end + 1;
            } else {
                break;
            }
        }

        while idx < len && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx + 1 >= len || !compact[idx..].to_ascii_lowercase().starts_with("as ") {
            break;
        }

        // Skip "as"
        idx += 2;
        while idx < len && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= len || bytes[idx] != b'(' {
            break;
        }

        let open = idx;
        let Some(close) = find_matching_paren(&compact, open) else {
            break;
        };
        let body = compact[open + 1..close].trim().to_string();
        ctes.insert(cte_name, body);
        idx = close + 1;

        while idx < len && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx < len && bytes[idx] == b',' {
            idx += 1;
            continue;
        }
        break;
    }

    let outer_query = compact[idx..].trim().to_string();
    if outer_query.is_empty() {
        (ctes, compact)
    } else {
        (ctes, outer_query)
    }
}

fn strip_leading_template_blocks(sql: &str) -> String {
    let mut s = sql.trim_start().to_string();

    loop {
        let trimmed = s.trim_start();
        if trimmed.starts_with("{{") {
            if let Some(end) = trimmed.find("}}") {
                s = trimmed[end + 2..].to_string();
                continue;
            }
        } else if trimmed.starts_with("{%") {
            if let Some(end) = trimmed.find("%}") {
                s = trimmed[end + 2..].to_string();
                continue;
            }
        } else if trimmed.starts_with("--") {
            if let Some(end) = trimmed.find('\n') {
                s = trimmed[end + 1..].to_string();
                continue;
            }
        } else if trimmed.starts_with("/*") {
            if let Some(end) = trimmed.find("*/") {
                s = trimmed[end + 2..].to_string();
                continue;
            }
        }
        return trimmed.to_string();
    }
}

fn resolve_star_select_target(query: &str, ctes: &HashMap<String, String>, depth: usize) -> String {
    if depth > 5 {
        return query.to_string();
    }

    let compact = normalize_whitespace(query);
    let lower = compact.to_ascii_lowercase();
    let Some(select_pos) = lower.find("select") else {
        return compact;
    };
    let Some(from_pos) = lower[select_pos..].find("from").map(|p| p + select_pos) else {
        return compact;
    };

    let select_clause = compact[select_pos + 6..from_pos].trim();
    if select_clause != "*" {
        return compact;
    }

    let after_from = compact[from_pos + 4..].trim();
    let source = after_from
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches(|c: char| matches!(c, ',' | ';' | '(' | ')'))
        .to_ascii_lowercase();

    if let Some(cte_sql) = ctes.get(&source) {
        return resolve_star_select_target(cte_sql, ctes, depth + 1);
    }

    compact
}

fn find_matching_paren(input: &str, open_idx: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    if bytes.get(open_idx).copied()? != b'(' {
        return None;
    }

    let mut depth = 0usize;
    for (i, ch) in bytes.iter().enumerate().skip(open_idx) {
        if *ch == b'(' {
            depth += 1;
        } else if *ch == b')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(i);
            }
        }
    }
    None
}

fn split_top_level(input: &str, sep: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();

    for ch in input.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            c if c == sep && depth == 0 => {
                out.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }

    out
}

fn parse_select_item(item: &str) -> (String, String) {
    let lower = item.to_ascii_lowercase();
    if let Some(pos) = lower.rfind(" as ") {
        let expr = item[..pos].trim().to_string();
        let output = item[pos + 4..].trim().to_string();
        return (output, expr);
    }

    let words: Vec<&str> = item.split_whitespace().collect();
    if words.len() > 1 {
        let output = words[words.len() - 1].to_string();
        let expr = words[..words.len() - 1].join(" ");
        return (output, expr);
    }

    (item.trim().to_string(), item.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::{explain_query, extract_lineage, extract_lineage_report, extract_tables};

    #[test]
    fn extracts_tables_from_from_and_join() {
        let sql = "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id";
        let tables = extract_tables(sql);
        assert_eq!(tables, vec!["orders", "customers"]);
    }

    #[test]
    fn extracts_basic_lineage() {
        let sql = "SELECT SUM(o.amount) AS revenue, o.customer_id FROM orders o";
        let lineage = extract_lineage(sql);
        assert_eq!(lineage[0].output, "revenue");
        assert_eq!(lineage[0].expression, "SUM(orders.amount)");
        assert_eq!(lineage[1].output, "o.customer_id");
        assert_eq!(lineage[1].expression, "orders.customer_id");
    }

    #[test]
    fn explains_query_with_aggregations() {
        let sql = "SELECT customer_id, SUM(amount) AS revenue FROM orders GROUP BY customer_id";
        let explanation = explain_query(sql);
        assert!(explanation.purpose.contains("aggregate"));
        assert_eq!(explanation.tables, vec!["orders"]);
        assert_eq!(explanation.aggregations, vec!["SUM"]);
        assert_eq!(
            explanation.aggregation_details,
            vec!["SUM(amount) AS revenue"]
        );
        assert_eq!(explanation.meaning, "revenue per customer");
    }

    #[test]
    fn explains_query_with_join_details() {
        let sql = "SELECT c.customer_id, SUM(o.amount) AS revenue FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.customer_id";
        let explanation = explain_query(sql);
        assert_eq!(explanation.tables, vec!["customers", "orders"]);
        assert_eq!(explanation.joins, vec!["customers.id = orders.customer_id"]);
        assert_eq!(explanation.meaning, "revenue per customer");
    }

    #[test]
    fn explains_query_with_contextual_dimension_labels() {
        let sql = "SELECT o.id, o.created_at, c.email, SUM(oi.quantity * oi.unit_price) AS total_amount FROM orders o JOIN customers c ON c.id = o.customer_id JOIN order_items oi ON oi.order_id = o.id GROUP BY o.id, o.created_at, c.email";
        let explanation = explain_query(sql);
        assert_eq!(
            explanation.meaning,
            "total amount per order id, date, and customer email"
        );
    }

    #[test]
    fn extracts_filter_and_join_lineage() {
        let sql = "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.created_at >= CURRENT_DATE - INTERVAL '7 days'";
        let report = extract_lineage_report(sql);

        assert_eq!(report.joins, vec!["orders.customer_id = customers.id"]);
        assert_eq!(
            report.filters,
            vec!["orders.created_at >= CURRENT_DATE - INTERVAL '7 days'"]
        );
    }

    #[test]
    fn follows_select_star_from_cte() {
        let sql = r#"
            WITH base AS (
                SELECT o.id, c.email
                FROM orders o
                JOIN customers c ON o.customer_id = c.id
                WHERE o.created_at >= CURRENT_DATE - INTERVAL '7 days'
            )
            SELECT * FROM base
        "#;

        let report = extract_lineage_report(sql);
        assert!(report
            .projections
            .iter()
            .any(|p| p.expression == "orders.id"));
        assert!(report
            .joins
            .iter()
            .any(|j| j == "orders.customer_id = customers.id"));
        assert!(report
            .filters
            .iter()
            .any(|f| f.contains("orders.created_at")));
    }

    #[test]
    fn handles_dbt_leading_config_block() {
        let sql = r#"
            {{ config(materialized="table") }}
            WITH base AS (
                SELECT proposals.proposal_id
                FROM proposals
                JOIN attachments ON proposals.proposal_id = attachments.attachable_id
            )
            SELECT * FROM base
        "#;

        let report = extract_lineage_report(sql);
        assert!(report
            .projections
            .iter()
            .any(|p| p.expression == "proposals.proposal_id"));
        assert!(report
            .joins
            .iter()
            .any(|j| j == "proposals.proposal_id = attachments.attachable_id"));
    }
}
