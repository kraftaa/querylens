#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticAnalysis {
    pub anti_patterns: Vec<String>,
    pub risks: Vec<String>,
    pub suggestions: Vec<String>,
    pub estimated_cost_impact: String,
}

pub fn analyze_sql(sql: &str) -> StaticAnalysis {
    let normalized = sql.to_ascii_lowercase();
    let join_count = normalized.matches(" join ").count();
    let mut anti_patterns = Vec::new();
    let mut risks = Vec::new();
    let mut suggestions = Vec::new();
    let mut score = 0;

    if normalized.contains("select *") {
        anti_patterns.push("SELECT *".to_string());
        risks.push("SELECT * can scan unnecessary columns and increase cost".to_string());
        suggestions.push("Project only the columns you need".to_string());
        score += 2;
    }

    if join_count >= 3 {
        anti_patterns.push(format!("{join_count} joins"));
        risks.push("Multiple joins can create large intermediate datasets".to_string());
        suggestions
            .push("Validate join cardinality and pre-aggregate before wide joins".to_string());
        score += 2;
    } else if join_count >= 2 {
        anti_patterns.push("Multiple joins".to_string());
        risks.push("Several joins can increase scan cost and shuffle volume".to_string());
        suggestions
            .push("Confirm each join is necessary and backed by selective predicates".to_string());
        score += 1;
    }

    if normalized.contains(" like '%") {
        anti_patterns.push("Leading wildcard LIKE".to_string());
        risks.push("Leading wildcard LIKE predicates often prevent efficient pruning".to_string());
        suggestions
            .push("Avoid leading wildcards or use a search-specific index/system".to_string());
        score += 2;
    }

    if !normalized.contains(" where ") {
        anti_patterns.push("No WHERE clause".to_string());
        risks.push("No WHERE clause may trigger a full table scan".to_string());
        suggestions
            .push("Add selective predicates or a partition filter when possible".to_string());
        score += 2;
    }

    if looks_exploratory_select(&normalized, join_count) && !normalized.contains(" limit ") {
        suggestions.push("Consider adding a LIMIT during ad hoc exploration".to_string());
    }

    if normalized.contains("cross join") {
        anti_patterns.push("CROSS JOIN".to_string());
        risks.push("CROSS JOIN can explode row counts and query cost".to_string());
        suggestions.push(
            "Replace CROSS JOIN with keyed joins unless a Cartesian product is intentional"
                .to_string(),
        );
        score += 3;
    }

    let estimated_cost_impact = match score {
        0..=1 => "low",
        2..=4 => "medium",
        _ => "high",
    }
    .to_string();

    StaticAnalysis {
        anti_patterns,
        risks,
        suggestions,
        estimated_cost_impact,
    }
}

fn looks_exploratory_select(normalized: &str, join_count: usize) -> bool {
    normalized.starts_with("select")
        && join_count == 0
        && !normalized.contains(" group by ")
        && !normalized.contains(" order by ")
        && !normalized.contains(" union ")
        && !normalized.contains(" with ")
        && !normalized.contains(" insert ")
        && !normalized.contains(" create ")
        && !normalized.contains(" merge ")
        && !normalized.contains(" update ")
        && !normalized.contains(" delete ")
}

#[cfg(test)]
mod tests {
    use super::analyze_sql;

    #[test]
    fn detects_common_sql_anti_patterns() {
        let sql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id JOIN regions r ON c.region_id = r.id WHERE c.email LIKE '%@example.com'";
        let analysis = analyze_sql(sql);

        assert!(analysis.anti_patterns.iter().any(|x| x == "SELECT *"));
        assert!(analysis.anti_patterns.iter().any(|x| x == "Multiple joins"));
        assert!(analysis
            .anti_patterns
            .iter()
            .any(|x| x == "Leading wildcard LIKE"));
        assert_eq!(analysis.estimated_cost_impact, "high");
    }

    #[test]
    fn identifies_low_risk_query() {
        let sql = "SELECT id, created_at FROM orders WHERE created_at >= CURRENT_DATE - INTERVAL '7 days' LIMIT 100";
        let analysis = analyze_sql(sql);

        assert!(analysis.anti_patterns.is_empty());
        assert_eq!(analysis.estimated_cost_impact, "low");
    }

    #[test]
    fn missing_limit_is_only_a_soft_suggestion() {
        let sql = "SELECT id, created_at FROM orders WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'";
        let analysis = analyze_sql(sql);

        assert!(!analysis.anti_patterns.iter().any(|x| x == "No LIMIT"));
        assert!(analysis
            .suggestions
            .iter()
            .any(|x| x == "Consider adding a LIMIT during ad hoc exploration"));
        assert_eq!(analysis.estimated_cost_impact, "low");
    }

    #[test]
    fn missing_limit_is_not_suggested_for_more_analytical_queries() {
        let sql = "SELECT o.id, c.email FROM orders o JOIN customers c ON o.customer_id = c.id ORDER BY o.created_at DESC";
        let analysis = analyze_sql(sql);

        assert!(!analysis
            .suggestions
            .iter()
            .any(|x| x == "Consider adding a LIMIT during ad hoc exploration"));
    }
}
