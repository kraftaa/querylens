use crate::prompt::{Finding, Severity};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DbtAuditOptions {
    pub fan_in_threshold: usize,
    pub fan_out_threshold: usize,
    pub domain_coupling_threshold: usize,
    pub hotspot_threshold: u32,
}

impl Default for DbtAuditOptions {
    fn default() -> Self {
        Self {
            fan_in_threshold: 8,
            fan_out_threshold: 8,
            domain_coupling_threshold: 6,
            hotspot_threshold: 18,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DbtHotspot {
    pub node_id: String,
    pub node_name: String,
    pub layer: String,
    pub domain: String,
    pub fan_in: usize,
    pub fan_out: usize,
    pub reverse_layer_edges: usize,
    pub cross_domain_edges: usize,
    pub score: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbtAuditSummary {
    pub total_nodes: usize,
    pub model_nodes: usize,
    pub dependency_edges: usize,
    pub layer_violations: usize,
    pub marts_depending_on_marts: usize,
    pub extreme_fan_in_nodes: usize,
    pub extreme_fan_out_nodes: usize,
    pub coupled_domain_pairs: usize,
    pub structural_hotspots: usize,
    pub complexity_score: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbtAuditReport {
    pub manifest: String,
    pub summary: DbtAuditSummary,
    pub findings: Vec<Finding>,
    pub hotspots: Vec<DbtHotspot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbtHotspotDelta {
    pub node_id: String,
    pub node_name: String,
    pub score_from: u32,
    pub score_to: u32,
    pub delta: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbtPrReviewSummary {
    pub status: String,
    pub complexity_from: u32,
    pub complexity_to: u32,
    pub complexity_delta: i64,
    pub new_findings: usize,
    pub resolved_findings: usize,
    pub new_layer_violations: usize,
    pub new_mart_on_mart_edges: usize,
    pub worsened_hotspots: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbtPrReviewReport {
    pub base_manifest: String,
    pub new_manifest: String,
    pub summary: DbtPrReviewSummary,
    pub new_findings: Vec<Finding>,
    pub resolved_findings: Vec<Finding>,
    pub worsened_hotspots: Vec<DbtHotspotDelta>,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    #[serde(default)]
    nodes: HashMap<String, ManifestNode>,
    #[serde(default)]
    sources: HashMap<String, ManifestNode>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ManifestDependsOn {
    #[serde(default)]
    nodes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ManifestNode {
    #[serde(default)]
    unique_id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    original_file_path: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    fqn: Vec<String>,
    #[serde(default)]
    depends_on: ManifestDependsOn,
}

#[derive(Debug, Clone)]
struct NodeInfo {
    unique_id: String,
    name: String,
    resource_type: String,
    layer: String,
    domain: String,
    parents: Vec<String>,
}

#[derive(Debug, Clone)]
struct AuditComputation {
    report: DbtAuditReport,
    hotspots_by_node: HashMap<String, DbtHotspot>,
}

pub fn audit_manifest(path: &Path, options: DbtAuditOptions) -> anyhow::Result<DbtAuditReport> {
    let manifest = load_manifest(path)?;
    Ok(audit_manifest_data(&manifest, options, path.display().to_string()).report)
}

pub fn dbt_pr_review(
    base_manifest: &Path,
    new_manifest: &Path,
    options: DbtAuditOptions,
) -> anyhow::Result<DbtPrReviewReport> {
    let base = load_manifest(base_manifest)?;
    let new = load_manifest(new_manifest)?;

    let base_report = audit_manifest_data(&base, options, base_manifest.display().to_string());
    let new_report = audit_manifest_data(&new, options, new_manifest.display().to_string());

    let base_findings = findings_by_key(&base_report.report.findings);
    let new_findings = findings_by_key(&new_report.report.findings);

    let mut introduced = new_findings
        .iter()
        .filter(|(k, _)| !base_findings.contains_key(*k))
        .map(|(_, v)| v.clone())
        .collect::<Vec<_>>();
    sort_findings(&mut introduced);

    let mut resolved = base_findings
        .iter()
        .filter(|(k, _)| !new_findings.contains_key(*k))
        .map(|(_, v)| v.clone())
        .collect::<Vec<_>>();
    sort_findings(&mut resolved);

    let mut worsened_hotspots = Vec::new();
    let mut hotspot_nodes = BTreeSet::new();
    for node in base_report.hotspots_by_node.keys() {
        hotspot_nodes.insert(node.clone());
    }
    for node in new_report.hotspots_by_node.keys() {
        hotspot_nodes.insert(node.clone());
    }

    for node_id in hotspot_nodes {
        let before = base_report
            .hotspots_by_node
            .get(&node_id)
            .map(|h| h.score)
            .unwrap_or(0);
        let after = new_report
            .hotspots_by_node
            .get(&node_id)
            .map(|h| h.score)
            .unwrap_or(0);
        if after > before {
            let node_name = new_report
                .hotspots_by_node
                .get(&node_id)
                .or_else(|| base_report.hotspots_by_node.get(&node_id))
                .map(|h| h.node_name.clone())
                .unwrap_or_else(|| node_id.clone());
            worsened_hotspots.push(DbtHotspotDelta {
                node_id: node_id.clone(),
                node_name,
                score_from: before,
                score_to: after,
                delta: after as i64 - before as i64,
            });
        }
    }

    worsened_hotspots.sort_by(|a, b| {
        b.delta
            .cmp(&a.delta)
            .then_with(|| a.node_name.cmp(&b.node_name))
    });

    let complexity_from = base_report.report.summary.complexity_score;
    let complexity_to = new_report.report.summary.complexity_score;
    let complexity_delta = complexity_to as i64 - complexity_from as i64;

    let new_layer_violations = introduced
        .iter()
        .filter(|f| f.rule_id == "LAYER_VIOLATION")
        .map(|f| f.evidence.len().max(1))
        .sum();
    let new_mart_on_mart_edges = introduced
        .iter()
        .filter(|f| f.rule_id == "MART_ON_MART_DEP")
        .map(|f| f.evidence.len().max(1))
        .sum();

    let fail = complexity_delta > 0
        || new_layer_violations > 0
        || new_mart_on_mart_edges > 0
        || introduced.iter().any(|f| f.severity == Severity::High)
        || !worsened_hotspots.is_empty();

    Ok(DbtPrReviewReport {
        base_manifest: base_manifest.display().to_string(),
        new_manifest: new_manifest.display().to_string(),
        summary: DbtPrReviewSummary {
            status: if fail {
                "FAIL".to_string()
            } else {
                "PASS".to_string()
            },
            complexity_from,
            complexity_to,
            complexity_delta,
            new_findings: introduced.len(),
            resolved_findings: resolved.len(),
            new_layer_violations,
            new_mart_on_mart_edges,
            worsened_hotspots: worsened_hotspots.len(),
        },
        new_findings: introduced,
        resolved_findings: resolved,
        worsened_hotspots,
    })
}

pub fn render_dbt_audit(report: &DbtAuditReport, top_findings: usize) -> String {
    let mut out = String::new();
    out.push_str("DBT Structural Audit\n");
    out.push_str("Manifest: ");
    out.push_str(&report.manifest);
    out.push_str("\n\n");

    out.push_str("Summary\n");
    out.push_str(&format!(
        "- Models: {}\n- Dependency edges: {}\n- Complexity score: {}\n",
        report.summary.model_nodes,
        report.summary.dependency_edges,
        report.summary.complexity_score
    ));
    out.push_str(&format!(
        "- Layer violations: {}\n- Mart-on-mart dependencies: {}\n",
        report.summary.layer_violations, report.summary.marts_depending_on_marts
    ));
    out.push_str(&format!(
        "- Extreme fan-in: {}\n- Extreme fan-out: {}\n- Coupled domains: {}\n- Structural hotspots: {}\n",
        report.summary.extreme_fan_in_nodes,
        report.summary.extreme_fan_out_nodes,
        report.summary.coupled_domain_pairs,
        report.summary.structural_hotspots
    ));

    out.push_str("\nTop Hotspots\n");
    if report.hotspots.is_empty() {
        out.push_str("- none\n");
    } else {
        for hotspot in report.hotspots.iter().take(top_findings) {
            out.push_str(&format!(
                "- {} (score {}, fan-in {}, fan-out {}, domain {})\n",
                hotspot.node_name, hotspot.score, hotspot.fan_in, hotspot.fan_out, hotspot.domain
            ));
        }
    }

    out.push_str("\nActionable Findings\n");
    if report.findings.is_empty() {
        out.push_str("- No structural rule violations detected.\n");
    } else {
        for finding in report.findings.iter().take(top_findings) {
            out.push_str(&format!(
                "- [{}] {}: {}\n",
                severity_label(&finding.severity),
                finding.rule_id,
                finding.message
            ));
            if !finding.evidence.is_empty() {
                out.push_str("  Evidence: ");
                out.push_str(&finding.evidence.join(", "));
                out.push('\n');
            }
        }
    }

    out
}

pub fn render_dbt_pr_review(report: &DbtPrReviewReport, top_findings: usize) -> String {
    let mut out = String::new();
    out.push_str("DBT PR Structural Review\n");
    out.push_str("Base manifest: ");
    out.push_str(&report.base_manifest);
    out.push('\n');
    out.push_str("New manifest: ");
    out.push_str(&report.new_manifest);
    out.push_str("\n\n");

    out.push_str("Status: ");
    out.push_str(&report.summary.status);
    out.push_str("\n\n");

    out.push_str("Summary\n");
    out.push_str(&format!(
        "- Complexity score: {} -> {} (delta {:+})\n",
        report.summary.complexity_from,
        report.summary.complexity_to,
        report.summary.complexity_delta
    ));
    out.push_str(&format!(
        "- New findings: {}\n- Resolved findings: {}\n",
        report.summary.new_findings, report.summary.resolved_findings
    ));
    out.push_str(&format!(
        "- New layer violations: {}\n- New mart-on-mart dependencies: {}\n",
        report.summary.new_layer_violations, report.summary.new_mart_on_mart_edges
    ));
    out.push_str(&format!(
        "- Worsened hotspots: {}\n",
        report.summary.worsened_hotspots
    ));

    out.push_str("\nIntroduced Findings\n");
    if report.new_findings.is_empty() {
        out.push_str("- none\n");
    } else {
        for finding in report.new_findings.iter().take(top_findings) {
            out.push_str(&format!(
                "- [{}] {}: {}\n",
                severity_label(&finding.severity),
                finding.rule_id,
                finding.message
            ));
        }
    }

    out.push_str("\nWorsened Hotspots\n");
    if report.worsened_hotspots.is_empty() {
        out.push_str("- none\n");
    } else {
        for hotspot in report.worsened_hotspots.iter().take(top_findings) {
            out.push_str(&format!(
                "- {} ({} -> {}, delta {:+})\n",
                hotspot.node_name, hotspot.score_from, hotspot.score_to, hotspot.delta
            ));
        }
    }

    out
}

fn severity_label(severity: &Severity) -> &'static str {
    match severity {
        Severity::Low => "LOW",
        Severity::Medium => "MEDIUM",
        Severity::High => "HIGH",
        Severity::Unknown => "UNKNOWN",
    }
}

fn load_manifest(path: &Path) -> anyhow::Result<Manifest> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            anyhow::anyhow!(
                "failed to read {}: {e}. Run `dbt compile` (or `dbt parse`) to generate target/manifest.json.",
                path.display()
            )
        } else {
            anyhow::anyhow!("failed to read {}: {e}", path.display())
        }
    })?;
    serde_json::from_str::<Manifest>(&raw).map_err(|e| {
        anyhow::anyhow!(
            "failed to parse {} as dbt manifest JSON: {e}",
            path.display()
        )
    })
}

fn audit_manifest_data(
    manifest: &Manifest,
    options: DbtAuditOptions,
    manifest_label: String,
) -> AuditComputation {
    let nodes = collect_nodes(manifest);
    let model_ids = nodes
        .values()
        .filter(|node| node.resource_type == "model")
        .map(|node| node.unique_id.clone())
        .collect::<HashSet<_>>();

    let mut parent_map = HashMap::<String, HashSet<String>>::new();
    let mut child_map = HashMap::<String, HashSet<String>>::new();
    let mut edges = Vec::<(String, String)>::new();

    for node in nodes.values() {
        let mut seen = HashSet::new();
        for parent in &node.parents {
            if !seen.insert(parent.clone()) {
                continue;
            }
            if !nodes.contains_key(parent) {
                continue;
            }
            edges.push((parent.clone(), node.unique_id.clone()));
            parent_map
                .entry(node.unique_id.clone())
                .or_default()
                .insert(parent.clone());
            child_map
                .entry(parent.clone())
                .or_default()
                .insert(node.unique_id.clone());
        }
    }

    edges.sort();
    edges.dedup();

    let mut findings = Vec::<Finding>::new();

    let mut layer_violation_map = BTreeMap::<String, Vec<String>>::new();
    let mut mart_on_mart_map = BTreeMap::<String, Vec<String>>::new();

    for (parent_id, child_id) in &edges {
        let Some(parent) = nodes.get(parent_id) else {
            continue;
        };
        let Some(child) = nodes.get(child_id) else {
            continue;
        };

        if is_forbidden_layer_edge(&parent.layer, &child.layer) {
            layer_violation_map
                .entry(child_id.clone())
                .or_default()
                .push(parent_id.clone());
        }

        if parent.layer == "marts"
            && child.layer == "marts"
            && parent.resource_type == "model"
            && child.resource_type == "model"
        {
            mart_on_mart_map
                .entry(child_id.clone())
                .or_default()
                .push(parent_id.clone());
        }
    }

    let mut layer_violation_edges = 0usize;
    for (child_id, mut parent_ids) in layer_violation_map {
        parent_ids.sort();
        parent_ids.dedup();
        layer_violation_edges += parent_ids.len();
        let child_name = node_display_name(&nodes, &child_id);
        let evidence = parent_ids
            .iter()
            .map(|parent_id| {
                let parent_name = node_display_name(&nodes, parent_id);
                format!("{parent_name} -> {child_name}")
            })
            .collect::<Vec<_>>();
        findings.push(Finding {
            rule_id: "LAYER_VIOLATION".to_string(),
            severity: Severity::High,
            message: format!(
                "{child_name} has {} forbidden layer edge(s)",
                parent_ids.len()
            ),
            why_it_matters: "Layer-policy violations increase coupling and make DAG ownership boundaries harder to maintain".to_string(),
            evidence,
        });
    }

    let mut mart_on_mart_edges = 0usize;
    for (child_id, mut parent_ids) in mart_on_mart_map {
        parent_ids.sort();
        parent_ids.dedup();
        mart_on_mart_edges += parent_ids.len();
        let child_name = node_display_name(&nodes, &child_id);
        let evidence = parent_ids
            .iter()
            .map(|parent_id| node_display_name(&nodes, parent_id))
            .collect::<Vec<_>>();
        findings.push(Finding {
            rule_id: "MART_ON_MART_DEP".to_string(),
            severity: if parent_ids.len() >= 3 {
                Severity::High
            } else {
                Severity::Medium
            },
            message: format!("{child_name} depends on {} mart model(s)", parent_ids.len()),
            why_it_matters:
                "Mart-on-mart chaining can hide ownership boundaries and increase coupling"
                    .to_string(),
            evidence,
        });
    }

    let mut hotspot_candidates = Vec::<DbtHotspot>::new();

    for model_id in &model_ids {
        let Some(node) = nodes.get(model_id) else {
            continue;
        };

        let parent_ids = parent_map
            .get(model_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        let child_ids = child_map
            .get(model_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();

        let fan_in = parent_ids.len();
        let fan_out = child_ids.len();

        if fan_in >= options.fan_in_threshold {
            let mut parent_names = parent_ids
                .iter()
                .map(|id| node_display_name(&nodes, id))
                .collect::<Vec<_>>();
            parent_names.sort();
            parent_names.truncate(8);

            findings.push(Finding {
                rule_id: "EXTREME_FAN_IN".to_string(),
                severity: if fan_in >= options.fan_in_threshold * 2 {
                    Severity::High
                } else {
                    Severity::Medium
                },
                message: format!("{} has fan-in {fan_in}", node.name),
                why_it_matters:
                    "High fan-in concentrates breakage risk and makes changes harder to validate"
                        .to_string(),
                evidence: parent_names,
            });
        }

        if fan_out >= options.fan_out_threshold {
            let mut child_names = child_ids
                .iter()
                .map(|id| node_display_name(&nodes, id))
                .collect::<Vec<_>>();
            child_names.sort();
            child_names.truncate(8);

            findings.push(Finding {
                rule_id: "EXTREME_FAN_OUT".to_string(),
                severity: if fan_out >= options.fan_out_threshold * 2 {
                    Severity::High
                } else {
                    Severity::Medium
                },
                message: format!("{} has fan-out {fan_out}", node.name),
                why_it_matters: "High fan-out increases blast radius when this model changes"
                    .to_string(),
                evidence: child_names,
            });
        }

        let mut reverse_layer_edges = 0usize;
        let mut cross_domain_edges = 0usize;

        for parent_id in &parent_ids {
            if let Some(parent) = nodes.get(parent_id) {
                if is_reverse_layer_dependency(&parent.layer, &node.layer) {
                    reverse_layer_edges += 1;
                }
                if is_known_domain(&parent.domain)
                    && is_known_domain(&node.domain)
                    && parent.domain != node.domain
                {
                    cross_domain_edges += 1;
                }
            }
        }

        for child_id in &child_ids {
            if let Some(child) = nodes.get(child_id) {
                if is_reverse_layer_dependency(&node.layer, &child.layer) {
                    reverse_layer_edges += 1;
                }
                if is_known_domain(&child.domain)
                    && is_known_domain(&node.domain)
                    && child.domain != node.domain
                {
                    cross_domain_edges += 1;
                }
            }
        }

        let mut score = fan_in as u32 + fan_out as u32;
        score += (cross_domain_edges as u32) * 2;
        score += (reverse_layer_edges as u32) * 3;

        if fan_in >= options.fan_in_threshold {
            score += 3;
        }
        if fan_out >= options.fan_out_threshold {
            score += 3;
        }

        if node.layer == "marts" {
            let mart_parent_count = parent_ids
                .iter()
                .filter_map(|id| nodes.get(id))
                .filter(|parent| parent.layer == "marts")
                .count();
            score += (mart_parent_count as u32) * 2;
        }

        if score >= options.hotspot_threshold {
            hotspot_candidates.push(DbtHotspot {
                node_id: node.unique_id.clone(),
                node_name: node.name.clone(),
                layer: node.layer.clone(),
                domain: node.domain.clone(),
                fan_in,
                fan_out,
                reverse_layer_edges,
                cross_domain_edges,
                score,
            });
        }
    }

    let mut cross_domain_pairs = BTreeMap::<(String, String), Vec<String>>::new();
    for (parent_id, child_id) in &edges {
        let Some(parent) = nodes.get(parent_id) else {
            continue;
        };
        let Some(child) = nodes.get(child_id) else {
            continue;
        };
        if !is_known_domain(&parent.domain)
            || !is_known_domain(&child.domain)
            || parent.domain == child.domain
        {
            continue;
        }

        let (left, right) = if parent.domain <= child.domain {
            (parent.domain.clone(), child.domain.clone())
        } else {
            (child.domain.clone(), parent.domain.clone())
        };

        cross_domain_pairs
            .entry((left, right))
            .or_default()
            .push(format!("{} -> {}", parent.name, child.name));
    }

    for ((left, right), mut links) in cross_domain_pairs {
        let count = links.len();
        if count < options.domain_coupling_threshold {
            continue;
        }

        links.sort();
        links.dedup();
        links.truncate(8);

        findings.push(Finding {
            rule_id: "CROSS_DOMAIN_COUPLING".to_string(),
            severity: if count >= options.domain_coupling_threshold * 2 {
                Severity::High
            } else {
                Severity::Medium
            },
            message: format!("Domains {left} and {right} have {count} cross-domain edges"),
            why_it_matters:
                "Heavy cross-domain dependencies can make teams tightly coupled and harder to change independently"
                    .to_string(),
            evidence: links,
        });
    }

    hotspot_candidates.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| b.fan_out.cmp(&a.fan_out))
            .then_with(|| a.node_name.cmp(&b.node_name))
    });

    for hotspot in &hotspot_candidates {
        findings.push(Finding {
            rule_id: "STRUCTURAL_HOTSPOT".to_string(),
            severity: if hotspot.score >= options.hotspot_threshold + 12 {
                Severity::High
            } else {
                Severity::Medium
            },
            message: format!(
                "{} is a structural hotspot (score {})",
                hotspot.node_name, hotspot.score
            ),
            why_it_matters:
                "Hotspots combine dependency concentration and coupling signals that often correlate with brittle DAG maintenance"
                    .to_string(),
            evidence: vec![format!(
                "fan-in={}, fan-out={}, reverse-layer-edges={}, cross-domain-edges={}",
                hotspot.fan_in,
                hotspot.fan_out,
                hotspot.reverse_layer_edges,
                hotspot.cross_domain_edges
            )],
        });
    }

    sort_findings(&mut findings);
    dedupe_findings(&mut findings);

    let layer_violations = layer_violation_edges;
    let mart_on_mart = mart_on_mart_edges;
    let extreme_fan_in = findings
        .iter()
        .filter(|f| f.rule_id == "EXTREME_FAN_IN")
        .count();
    let extreme_fan_out = findings
        .iter()
        .filter(|f| f.rule_id == "EXTREME_FAN_OUT")
        .count();
    let coupled_domain_pairs = findings
        .iter()
        .filter(|f| f.rule_id == "CROSS_DOMAIN_COUPLING")
        .count();
    let structural_hotspots = findings
        .iter()
        .filter(|f| f.rule_id == "STRUCTURAL_HOTSPOT")
        .count();

    let complexity_score = edges.len() as u32
        + (layer_violations as u32 * 3)
        + (mart_on_mart as u32 * 2)
        + extreme_fan_in as u32
        + extreme_fan_out as u32
        + (coupled_domain_pairs as u32 * 2)
        + (structural_hotspots as u32 * 2);

    let summary = DbtAuditSummary {
        total_nodes: nodes.len(),
        model_nodes: model_ids.len(),
        dependency_edges: edges.len(),
        layer_violations,
        marts_depending_on_marts: mart_on_mart,
        extreme_fan_in_nodes: extreme_fan_in,
        extreme_fan_out_nodes: extreme_fan_out,
        coupled_domain_pairs,
        structural_hotspots,
        complexity_score,
    };

    let mut hotspots_by_node = HashMap::new();
    for hotspot in &hotspot_candidates {
        hotspots_by_node.insert(hotspot.node_id.clone(), hotspot.clone());
    }

    AuditComputation {
        report: DbtAuditReport {
            manifest: manifest_label,
            summary,
            findings,
            hotspots: hotspot_candidates,
        },
        hotspots_by_node,
    }
}

fn collect_nodes(manifest: &Manifest) -> HashMap<String, NodeInfo> {
    let mut out = HashMap::new();

    for node in manifest.nodes.values().chain(manifest.sources.values()) {
        if node.unique_id.is_empty() {
            continue;
        }

        let resource_type = if node.resource_type.is_empty() {
            node.unique_id
                .split('.')
                .next()
                .unwrap_or("unknown")
                .to_string()
        } else {
            node.resource_type.to_ascii_lowercase()
        };

        if matches!(
            resource_type.as_str(),
            "test" | "analysis" | "exposure" | "metric"
        ) {
            continue;
        }

        let path = if !node.original_file_path.is_empty() {
            node.original_file_path.clone()
        } else {
            node.path.clone()
        };

        let name = if !node.name.is_empty() {
            node.name.clone()
        } else {
            node.unique_id
                .split('.')
                .next_back()
                .unwrap_or(node.unique_id.as_str())
                .to_string()
        };

        out.insert(
            node.unique_id.clone(),
            NodeInfo {
                unique_id: node.unique_id.clone(),
                name: name.clone(),
                resource_type: resource_type.clone(),
                layer: infer_layer(&path, &node.fqn, &name, &resource_type),
                domain: infer_domain(&path, &node.fqn),
                parents: node.depends_on.nodes.clone(),
            },
        );
    }

    out
}

fn infer_layer(path: &str, fqn: &[String], name: &str, resource_type: &str) -> String {
    if resource_type == "source" {
        return "source".to_string();
    }

    let path_l = path.to_ascii_lowercase().replace('\\', "/");
    let fqn_l = fqn
        .iter()
        .map(|s| s.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let name_l = name.to_ascii_lowercase();

    if path_l.contains("/marts/")
        || fqn_l.iter().any(|s| s == "marts")
        || name_l.starts_with("mart_")
    {
        return "marts".to_string();
    }

    if path_l.contains("/intermediate/")
        || fqn_l.iter().any(|s| s == "intermediate")
        || name_l.starts_with("int_")
    {
        return "intermediate".to_string();
    }

    if path_l.contains("/staging/")
        || fqn_l.iter().any(|s| s == "staging")
        || name_l.starts_with("stg_")
    {
        return "staging".to_string();
    }

    if path_l.contains("/base/") || fqn_l.iter().any(|s| s == "base") || name_l.starts_with("base_")
    {
        return "base".to_string();
    }

    "unknown".to_string()
}

fn infer_domain(path: &str, fqn: &[String]) -> String {
    let path_l = path.to_ascii_lowercase().replace('\\', "/");
    let parts = path_l
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();

    for idx in 0..parts.len() {
        if parts[idx] != "models" {
            continue;
        }
        let after = &parts[idx + 1..];
        if after.is_empty() {
            break;
        }
        if is_layer_name(after[0]) {
            if after.len() > 1 {
                return normalize_domain(after[1]);
            }
            break;
        }
        return normalize_domain(after[0]);
    }

    let fqn_l = fqn
        .iter()
        .map(|s| s.to_ascii_lowercase())
        .collect::<Vec<_>>();

    for idx in 0..fqn_l.len() {
        if is_layer_name(&fqn_l[idx]) && idx + 1 < fqn_l.len() {
            return normalize_domain(&fqn_l[idx + 1]);
        }
    }

    "unknown".to_string()
}

fn normalize_domain(raw: &str) -> String {
    raw.trim_end_matches(".sql")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
}

fn is_layer_name(value: &str) -> bool {
    matches!(value, "staging" | "intermediate" | "marts" | "base")
}

fn layer_rank(layer: &str) -> Option<u8> {
    match layer {
        "source" => Some(0),
        "base" => Some(1),
        "staging" => Some(2),
        "intermediate" => Some(3),
        "marts" => Some(4),
        _ => None,
    }
}

fn is_reverse_layer_dependency(parent_layer: &str, child_layer: &str) -> bool {
    let Some(parent_rank) = layer_rank(parent_layer) else {
        return false;
    };
    let Some(child_rank) = layer_rank(child_layer) else {
        return false;
    };

    parent_rank > child_rank
}

fn is_forbidden_layer_edge(parent_layer: &str, child_layer: &str) -> bool {
    match child_layer {
        "base" => parent_layer != "source",
        "staging" => !matches!(parent_layer, "source" | "base"),
        "intermediate" => !matches!(parent_layer, "staging" | "base" | "intermediate"),
        "marts" => !matches!(parent_layer, "intermediate" | "marts"),
        _ => false,
    }
}

fn is_known_domain(domain: &str) -> bool {
    domain != "unknown" && !domain.is_empty()
}

fn node_display_name(nodes: &HashMap<String, NodeInfo>, node_id: &str) -> String {
    nodes
        .get(node_id)
        .map(|node| node.name.clone())
        .unwrap_or_else(|| node_id.to_string())
}

fn sort_findings(findings: &mut [Finding]) {
    findings.sort_by(|a, b| {
        b.severity
            .rank()
            .cmp(&a.severity.rank())
            .then_with(|| a.rule_id.cmp(&b.rule_id))
            .then_with(|| a.message.cmp(&b.message))
    });
}

fn dedupe_findings(findings: &mut Vec<Finding>) {
    let mut seen = HashSet::new();
    findings.retain(|finding| {
        let mut evidence = finding.evidence.clone();
        evidence.sort();
        let key = format!(
            "{}|{}|{}|{}",
            finding.rule_id,
            finding.severity.rank(),
            finding.message,
            evidence.join(";")
        );
        seen.insert(key)
    });
}

fn findings_by_key(findings: &[Finding]) -> HashMap<String, Finding> {
    let mut out = HashMap::new();
    for finding in findings {
        let mut evidence = finding.evidence.clone();
        evidence.sort();
        let key = format!(
            "{}|{}|{}|{}",
            finding.rule_id,
            finding.severity.rank(),
            finding.message,
            evidence.join(";")
        );
        out.insert(key, finding.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{audit_manifest, dbt_pr_review, DbtAuditOptions};
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn dbt_audit_reports_actionable_findings() {
        let temp_dir = temp_test_dir("dbt-audit");
        let manifest_path = temp_dir.join("manifest.json");

        let manifest = json!({
            "nodes": {
                "model.demo.stg_orders": {
                    "unique_id": "model.demo.stg_orders",
                    "name": "stg_orders",
                    "resource_type": "model",
                    "original_file_path": "models/staging/sales/stg_orders.sql",
                    "fqn": ["demo", "staging", "sales", "stg_orders"],
                    "depends_on": {"nodes": []}
                },
                "model.demo.stg_payments": {
                    "unique_id": "model.demo.stg_payments",
                    "name": "stg_payments",
                    "resource_type": "model",
                    "original_file_path": "models/staging/finance/stg_payments.sql",
                    "fqn": ["demo", "staging", "finance", "stg_payments"],
                    "depends_on": {"nodes": []}
                },
                "model.demo.int_customer_orders": {
                    "unique_id": "model.demo.int_customer_orders",
                    "name": "int_customer_orders",
                    "resource_type": "model",
                    "original_file_path": "models/intermediate/shared/int_customer_orders.sql",
                    "fqn": ["demo", "intermediate", "shared", "int_customer_orders"],
                    "depends_on": {"nodes": ["model.demo.stg_orders", "model.demo.stg_payments"]}
                },
                "model.demo.mart_finance": {
                    "unique_id": "model.demo.mart_finance",
                    "name": "mart_finance",
                    "resource_type": "model",
                    "original_file_path": "models/marts/finance/mart_finance.sql",
                    "fqn": ["demo", "marts", "finance", "mart_finance"],
                    "depends_on": {"nodes": ["model.demo.int_customer_orders"]}
                },
                "model.demo.mart_sales": {
                    "unique_id": "model.demo.mart_sales",
                    "name": "mart_sales",
                    "resource_type": "model",
                    "original_file_path": "models/marts/sales/mart_sales.sql",
                    "fqn": ["demo", "marts", "sales", "mart_sales"],
                    "depends_on": {"nodes": ["model.demo.int_customer_orders"]}
                },
                "model.demo.mart_support": {
                    "unique_id": "model.demo.mart_support",
                    "name": "mart_support",
                    "resource_type": "model",
                    "original_file_path": "models/marts/support/mart_support.sql",
                    "fqn": ["demo", "marts", "support", "mart_support"],
                    "depends_on": {"nodes": ["model.demo.int_customer_orders"]}
                },
                "model.demo.mart_profit": {
                    "unique_id": "model.demo.mart_profit",
                    "name": "mart_profit",
                    "resource_type": "model",
                    "original_file_path": "models/marts/finance/mart_profit.sql",
                    "fqn": ["demo", "marts", "finance", "mart_profit"],
                    "depends_on": {"nodes": ["model.demo.mart_finance"]}
                },
                "model.demo.mart_direct_stage": {
                    "unique_id": "model.demo.mart_direct_stage",
                    "name": "mart_direct_stage",
                    "resource_type": "model",
                    "original_file_path": "models/marts/finance/mart_direct_stage.sql",
                    "fqn": ["demo", "marts", "finance", "mart_direct_stage"],
                    "depends_on": {"nodes": ["model.demo.stg_orders"]}
                },
                "model.demo.stg_bad_backflow": {
                    "unique_id": "model.demo.stg_bad_backflow",
                    "name": "stg_bad_backflow",
                    "resource_type": "model",
                    "original_file_path": "models/staging/sales/stg_bad_backflow.sql",
                    "fqn": ["demo", "staging", "sales", "stg_bad_backflow"],
                    "depends_on": {"nodes": ["model.demo.mart_finance"]}
                }
            }
        });

        std::fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&manifest).expect("serialize"),
        )
        .expect("write manifest");

        let report = audit_manifest(
            &manifest_path,
            DbtAuditOptions {
                fan_in_threshold: 2,
                fan_out_threshold: 2,
                domain_coupling_threshold: 2,
                hotspot_threshold: 6,
            },
        )
        .expect("audit should run");

        let rule_ids = report
            .findings
            .iter()
            .map(|f| f.rule_id.as_str())
            .collect::<Vec<_>>();

        assert!(rule_ids.contains(&"LAYER_VIOLATION"));
        assert!(rule_ids.contains(&"MART_ON_MART_DEP"));
        assert!(rule_ids.contains(&"EXTREME_FAN_IN"));
        assert!(rule_ids.contains(&"EXTREME_FAN_OUT"));
        assert!(rule_ids.contains(&"CROSS_DOMAIN_COUPLING"));
        assert!(rule_ids.contains(&"STRUCTURAL_HOTSPOT"));
        assert!(report
            .findings
            .iter()
            .any(|f| f.rule_id == "LAYER_VIOLATION"
                && f.evidence
                    .iter()
                    .any(|e| e.contains("stg_orders -> mart_direct_stage"))));
        assert!(report.summary.complexity_score > 0);

        std::fs::remove_dir_all(temp_dir).expect("cleanup");
    }

    #[test]
    fn dbt_pr_review_detects_complexity_regression() {
        let temp_dir = temp_test_dir("dbt-pr");
        let base_path = temp_dir.join("base_manifest.json");
        let new_path = temp_dir.join("new_manifest.json");

        let base = json!({
            "nodes": {
                "model.demo.stg_orders": {
                    "unique_id": "model.demo.stg_orders",
                    "name": "stg_orders",
                    "resource_type": "model",
                    "original_file_path": "models/staging/sales/stg_orders.sql",
                    "fqn": ["demo", "staging", "sales", "stg_orders"],
                    "depends_on": {"nodes": []}
                },
                "model.demo.int_orders": {
                    "unique_id": "model.demo.int_orders",
                    "name": "int_orders",
                    "resource_type": "model",
                    "original_file_path": "models/intermediate/shared/int_orders.sql",
                    "fqn": ["demo", "intermediate", "shared", "int_orders"],
                    "depends_on": {"nodes": ["model.demo.stg_orders"]}
                },
                "model.demo.mart_revenue": {
                    "unique_id": "model.demo.mart_revenue",
                    "name": "mart_revenue",
                    "resource_type": "model",
                    "original_file_path": "models/marts/finance/mart_revenue.sql",
                    "fqn": ["demo", "marts", "finance", "mart_revenue"],
                    "depends_on": {"nodes": ["model.demo.int_orders"]}
                }
            }
        });

        let new = json!({
            "nodes": {
                "model.demo.stg_orders": {
                    "unique_id": "model.demo.stg_orders",
                    "name": "stg_orders",
                    "resource_type": "model",
                    "original_file_path": "models/staging/sales/stg_orders.sql",
                    "fqn": ["demo", "staging", "sales", "stg_orders"],
                    "depends_on": {"nodes": []}
                },
                "model.demo.int_orders": {
                    "unique_id": "model.demo.int_orders",
                    "name": "int_orders",
                    "resource_type": "model",
                    "original_file_path": "models/intermediate/shared/int_orders.sql",
                    "fqn": ["demo", "intermediate", "shared", "int_orders"],
                    "depends_on": {"nodes": ["model.demo.stg_orders"]}
                },
                "model.demo.mart_revenue": {
                    "unique_id": "model.demo.mart_revenue",
                    "name": "mart_revenue",
                    "resource_type": "model",
                    "original_file_path": "models/marts/finance/mart_revenue.sql",
                    "fqn": ["demo", "marts", "finance", "mart_revenue"],
                    "depends_on": {"nodes": ["model.demo.int_orders"]}
                },
                "model.demo.mart_profit": {
                    "unique_id": "model.demo.mart_profit",
                    "name": "mart_profit",
                    "resource_type": "model",
                    "original_file_path": "models/marts/finance/mart_profit.sql",
                    "fqn": ["demo", "marts", "finance", "mart_profit"],
                    "depends_on": {"nodes": ["model.demo.mart_revenue"]}
                },
                "model.demo.stg_backflow": {
                    "unique_id": "model.demo.stg_backflow",
                    "name": "stg_backflow",
                    "resource_type": "model",
                    "original_file_path": "models/staging/sales/stg_backflow.sql",
                    "fqn": ["demo", "staging", "sales", "stg_backflow"],
                    "depends_on": {"nodes": ["model.demo.mart_revenue"]}
                }
            }
        });

        std::fs::write(
            &base_path,
            serde_json::to_string_pretty(&base).expect("serialize"),
        )
        .expect("write base");
        std::fs::write(
            &new_path,
            serde_json::to_string_pretty(&new).expect("serialize"),
        )
        .expect("write new");

        let report = dbt_pr_review(
            &base_path,
            &new_path,
            DbtAuditOptions {
                fan_in_threshold: 2,
                fan_out_threshold: 2,
                domain_coupling_threshold: 2,
                hotspot_threshold: 4,
            },
        )
        .expect("pr review should run");

        assert_eq!(report.summary.status, "FAIL");
        assert!(report.summary.complexity_delta > 0);
        assert!(report.summary.new_findings > 0);
        assert!(report.summary.new_layer_violations > 0);

        std::fs::remove_dir_all(temp_dir).expect("cleanup");
    }

    fn temp_test_dir(label: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix time")
            .as_nanos();
        let dir = base.join(format!(
            "querylens-dbt-test-{label}-{}-{nanos}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }
}
