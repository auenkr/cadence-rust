//! Advanced-visibility query layer: port of Go `common/elasticsearch/validator`,
//! the cch123/elasticsql SQL->DSL conversion and Cadence's
//! `es_visibility_store.go` post-processing (page tokens, sort, search_after,
//! time/close-status value conversion, ListOpen/ListClosed bodies).
//!
//! Pure logic (no I/O) so it is unit-testable; the HTTP client lives in
//! `es_visibility.rs`.

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

/// Error classification mirroring the Go error types the callers map to.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryError {
    BadRequest(String),
    Internal(String),
}

fn bad(m: impl Into<String>) -> QueryError {
    QueryError::BadRequest(m.into())
}

// ---------------------------------------------------------------- tokenizer

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Back(String),
    Str(String),
    Num(String),
    Op(String),
    LParen,
    RParen,
    Comma,
    Star,
}

fn tokenize(s: &str) -> Result<Vec<Tok>, ()> {
    let c: Vec<char> = s.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '\'' || ch == '"' {
            let q = ch;
            i += 1;
            let mut v = String::new();
            loop {
                if i >= c.len() {
                    return Err(());
                }
                let d = c[i];
                if d == q {
                    if i + 1 < c.len() && c[i + 1] == q {
                        v.push(q);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                if d == '\\' {
                    if i + 1 >= c.len() {
                        return Err(());
                    }
                    let e = c[i + 1];
                    v.push(match e {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        '0' => '\0',
                        o => o,
                    });
                    i += 2;
                    continue;
                }
                v.push(d);
                i += 1;
            }
            out.push(Tok::Str(v));
        } else if ch == '`' {
            i += 1;
            let mut v = String::new();
            loop {
                if i >= c.len() {
                    return Err(());
                }
                if c[i] == '`' {
                    i += 1;
                    break;
                }
                v.push(c[i]);
                i += 1;
            }
            out.push(Tok::Back(v));
        } else if ch.is_ascii_digit()
            || (ch == '-' && i + 1 < c.len() && c[i + 1].is_ascii_digit() && !prev_is_value(&out))
        {
            let st = i;
            i += 1;
            while i < c.len() && (c[i].is_ascii_digit() || c[i] == '.') {
                i += 1;
            }
            out.push(Tok::Num(c[st..i].iter().collect()));
        } else if ch.is_alphabetic() || ch == '_' || ch == '$' || ch == '@' {
            let st = i;
            while i < c.len()
                && (c[i].is_alphanumeric()
                    || c[i] == '_'
                    || c[i] == '$'
                    || c[i] == '.'
                    || c[i] == '@')
            {
                i += 1;
            }
            out.push(Tok::Ident(c[st..i].iter().collect()));
        } else if ch == '(' {
            out.push(Tok::LParen);
            i += 1;
        } else if ch == ')' {
            out.push(Tok::RParen);
            i += 1;
        } else if ch == ',' {
            out.push(Tok::Comma);
            i += 1;
        } else if ch == '*' {
            out.push(Tok::Star);
            i += 1;
        } else if matches!(ch, '=' | '!' | '<' | '>') {
            let mut op = String::from(ch);
            if i + 1 < c.len() && matches!(c[i + 1], '=' | '>') {
                op.push(c[i + 1]);
                i += 1;
                if op == "<=" && i + 1 < c.len() && c[i + 1] == '>' {
                    op.push('>');
                    i += 1;
                }
            }
            i += 1;
            if op == "!" {
                return Err(());
            }
            if op == "<>" {
                op = "!=".into();
            }
            out.push(Tok::Op(op));
        } else {
            return Err(());
        }
    }
    Ok(out)
}

fn prev_is_value(out: &[Tok]) -> bool {
    matches!(
        out.last(),
        Some(Tok::Num(_))
            | Some(Tok::Str(_))
            | Some(Tok::Ident(_))
            | Some(Tok::Back(_))
            | Some(Tok::RParen)
    )
}

// ------------------------------------------------------------------- parser

#[derive(Debug, Clone, PartialEq)]
enum Operand {
    Col(String, bool), // name, backticked
    Val(String, bool), // raw value, quoted
    Tuple(Vec<(String, bool)>),
    Other(String),
}

#[derive(Debug, Clone, PartialEq)]
enum Expr {
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Paren(Box<Expr>),
    Cmp(Operand, String, Operand),
    Range(Operand, bool, Operand, Operand),
    Is,
}

#[derive(Debug, Default)]
struct Parsed {
    where_: Option<Expr>,
    order: Vec<(Operand, String)>,
    limit: Option<(i64, i64)>, // (offset, count)
}

struct P {
    t: Vec<Tok>,
    i: usize,
}

fn kw(t: &Tok, k: &str) -> bool {
    matches!(t, Tok::Ident(s) if s.eq_ignore_ascii_case(k))
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn peek_kw(&self, k: &str) -> bool {
        self.peek().map(|t| kw(t, k)).unwrap_or(false)
    }
    fn eat_kw(&mut self, k: &str) -> bool {
        if self.peek_kw(k) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn expr(&mut self) -> Result<Expr, ()> {
        self.or()
    }
    fn or(&mut self) -> Result<Expr, ()> {
        let mut l = self.and()?;
        while self.eat_kw("or") {
            let r = self.and()?;
            l = Expr::Or(Box::new(l), Box::new(r));
        }
        Ok(l)
    }
    fn and(&mut self) -> Result<Expr, ()> {
        let mut l = self.not()?;
        while self.eat_kw("and") {
            let r = self.not()?;
            l = Expr::And(Box::new(l), Box::new(r));
        }
        Ok(l)
    }
    fn not(&mut self) -> Result<Expr, ()> {
        if self.eat_kw("not") {
            let e = self.not()?;
            return Ok(Expr::Not(Box::new(e)));
        }
        self.cond()
    }
    fn operand(&mut self) -> Result<Operand, ()> {
        match self.peek().cloned() {
            Some(Tok::Back(s)) => {
                self.i += 1;
                Ok(Operand::Col(s, true))
            }
            Some(Tok::Str(s)) => {
                self.i += 1;
                Ok(Operand::Val(s, true))
            }
            Some(Tok::Num(s)) => {
                self.i += 1;
                Ok(Operand::Val(s, false))
            }
            Some(Tok::Ident(s)) => {
                self.i += 1;
                let l = s.to_ascii_lowercase();
                if l == "null" || l == "true" || l == "false" {
                    Ok(Operand::Other(l))
                } else {
                    Ok(Operand::Col(s, false))
                }
            }
            Some(Tok::LParen) => {
                // value tuple
                self.i += 1;
                let mut items = Vec::new();
                loop {
                    match self.peek().cloned() {
                        Some(Tok::Str(s)) => items.push((s, true)),
                        Some(Tok::Num(s)) => items.push((s, false)),
                        _ => return Err(()),
                    }
                    self.i += 1;
                    match self.peek() {
                        Some(Tok::Comma) => self.i += 1,
                        Some(Tok::RParen) => {
                            self.i += 1;
                            break;
                        }
                        _ => return Err(()),
                    }
                }
                Ok(Operand::Tuple(items))
            }
            _ => Err(()),
        }
    }
    fn cond(&mut self) -> Result<Expr, ()> {
        if matches!(self.peek(), Some(Tok::LParen)) {
            // parenthesised expression (a tuple can only follow in/not in)
            self.i += 1;
            let e = self.expr()?;
            if !matches!(self.peek(), Some(Tok::RParen)) {
                return Err(());
            }
            self.i += 1;
            return Ok(Expr::Paren(Box::new(e)));
        }
        let l = self.operand()?;
        let neg = {
            let save = self.i;
            if self.eat_kw("not") {
                if self.peek_kw("in") || self.peek_kw("like") || self.peek_kw("between") {
                    true
                } else {
                    self.i = save;
                    return Err(());
                }
            } else {
                false
            }
        };
        if self.eat_kw("between") {
            let a = self.operand()?;
            if !self.eat_kw("and") {
                return Err(());
            }
            let b = self.operand()?;
            return Ok(Expr::Range(l, neg, a, b));
        }
        if self.eat_kw("in") {
            let r = self.operand()?;
            if !matches!(r, Operand::Tuple(_)) {
                return Err(());
            }
            return Ok(Expr::Cmp(l, if neg { "not in" } else { "in" }.into(), r));
        }
        if self.eat_kw("like") {
            let r = self.operand()?;
            return Ok(Expr::Cmp(
                l,
                if neg { "not like" } else { "like" }.into(),
                r,
            ));
        }
        if self.eat_kw("is") {
            self.eat_kw("not");
            let _ = self.operand()?;
            return Ok(Expr::Is);
        }
        if let Some(Tok::Op(op)) = self.peek().cloned() {
            self.i += 1;
            let r = self.operand()?;
            return Ok(Expr::Cmp(l, op, r));
        }
        // A bare operand is valid SQL for Go's parser; the validator then
        // rejects it as "invalid where clause".
        if self.peek().is_none()
            || matches!(self.peek(), Some(Tok::RParen))
            || self.peek_kw("and")
            || self.peek_kw("or")
            || self.peek_kw("order")
            || self.peek_kw("limit")
        {
            return Ok(Expr::Is);
        }
        Err(())
    }
}

fn parse_clause(s: &str) -> Result<Parsed, ()> {
    let t = tokenize(s)?;
    let mut p = P { t, i: 0 };
    let mut out = Parsed::default();
    if !p.peek_kw("order") && !p.peek_kw("limit") && p.peek().is_some() {
        out.where_ = Some(p.expr()?);
    }
    if p.eat_kw("order") {
        if !p.eat_kw("by") {
            return Err(());
        }
        loop {
            let o = p.operand()?;
            let dir = if p.eat_kw("desc") {
                "desc"
            } else {
                p.eat_kw("asc");
                "asc"
            };
            out.order.push((o, dir.into()));
            if matches!(p.peek(), Some(Tok::Comma)) {
                p.i += 1;
            } else {
                break;
            }
        }
    }
    if p.eat_kw("limit") {
        let n1 = match p.peek().cloned() {
            Some(Tok::Num(n)) => n.parse::<i64>().map_err(|_| ())?,
            _ => return Err(()),
        };
        p.i += 1;
        if matches!(p.peek(), Some(Tok::Comma)) {
            p.i += 1;
            let n2 = match p.peek().cloned() {
                Some(Tok::Num(n)) => n.parse::<i64>().map_err(|_| ())?,
                _ => return Err(()),
            };
            p.i += 1;
            out.limit = Some((n1, n2));
        } else {
            out.limit = Some((0, n1));
        }
    }
    if p.peek().is_some() {
        return Err(());
    }
    Ok(out)
}

// --------------------------------------------------------------- formatting

fn esc_sql(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '\'' => o.push_str("\\'"),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            '\0' => o.push_str("\\0"),
            c => o.push(c),
        }
    }
    o
}

fn fmt_col(name: &str, _back: bool) -> String {
    if name.contains('.') {
        format!("`{name}`")
    } else {
        name.to_string()
    }
}

fn fmt_operand(o: &Operand) -> String {
    match o {
        Operand::Col(n, b) => fmt_col(n, *b),
        Operand::Val(v, true) => format!("'{}'", esc_sql(v)),
        Operand::Val(v, false) => v.clone(),
        Operand::Tuple(items) => {
            let v: Vec<String> = items
                .iter()
                .map(|(v, q)| {
                    if *q {
                        format!("'{}'", esc_sql(v))
                    } else {
                        v.clone()
                    }
                })
                .collect();
            format!("({})", v.join(", "))
        }
        Operand::Other(s) => s.clone(),
    }
}

fn fmt_expr(e: &Expr) -> String {
    match e {
        Expr::And(l, r) => format!("{} and {}", fmt_expr(l), fmt_expr(r)),
        Expr::Or(l, r) => format!("{} or {}", fmt_expr(l), fmt_expr(r)),
        Expr::Not(x) => format!("not {}", fmt_expr(x)),
        Expr::Paren(x) => format!("({})", fmt_expr(x)),
        Expr::Cmp(l, op, r) => format!("{} {} {}", fmt_operand(l), op, fmt_operand(r)),
        Expr::Range(l, n, a, b) => format!(
            "{} {}between {} and {}",
            fmt_operand(l),
            if *n { "not " } else { "" },
            fmt_operand(a),
            fmt_operand(b)
        ),
        Expr::Is => "is".into(),
    }
}

// ---------------------------------------------------------------- validator

fn is_system_key(k: &str) -> bool {
    cadence_api::search_attributes::is_system_indexed_key(k)
}

fn is_union_select(clause: &str) -> bool {
    let Ok(t) = tokenize(clause) else {
        return false;
    };
    let Some(u) = t.iter().position(|x| kw(x, "union")) else {
        return false;
    };
    let mut j = u + 1;
    if t.get(j).is_some_and(|x| kw(x, "all") || kw(x, "distinct")) {
        j += 1;
    }
    t.get(j).is_some_and(|x| kw(x, "select")) && parse_clause_prefix_ok(&t[..u])
}

fn parse_clause_prefix_ok(t: &[Tok]) -> bool {
    let mut p = P {
        t: t.to_vec(),
        i: 0,
    };
    p.expr().is_ok() && p.peek().is_none()
}

/// Port of `VisibilityQueryValidator.ValidateQuery`.
pub fn validate_query<V>(
    clause: &str,
    valid_attrs: &HashMap<String, V>,
    enable_validation: bool,
) -> Result<String, QueryError> {
    let clause = clause.trim();
    if clause.is_empty() {
        return Ok(String::new());
    }
    let mut parsed = parse_clause(clause).map_err(|_| {
        // Go's parser accepts `<where> UNION SELECT ...` as a Union statement,
        // which the validator reports as "Invalid select query.".
        if is_union_select(clause) {
            bad("Invalid select query.")
        } else {
            bad("Invalid query.")
        }
    })?;
    let valid = |k: &str| !enable_validation || valid_attrs.contains_key(k);
    let mut buf = String::new();
    if let Some(w) = parsed.where_.as_mut() {
        validate_where(w, &valid)?;
        buf.push_str(&fmt_expr(w));
    }
    if !parsed.order.is_empty() {
        let mut parts = Vec::new();
        for (o, dir) in parsed.order.iter() {
            let name = match o {
                Operand::Col(n, _) => n,
                _ => return Err(bad("invalid order by expression")),
            };
            if !valid(name) {
                return Err(bad("invalid order by attribute"));
            }
            let full = if is_system_key(name) {
                name.clone()
            } else {
                format!("Attr.{name}")
            };
            parts.push(format!("{} {}", fmt_col(&full, false), dir));
        }
        buf.push_str(" order by ");
        buf.push_str(&parts.join(", "));
    }
    Ok(buf)
}

fn validate_where(e: &mut Expr, valid: &dyn Fn(&str) -> bool) -> Result<(), QueryError> {
    match e {
        Expr::And(l, r) | Expr::Or(l, r) => {
            validate_where(l, valid)?;
            validate_where(r, valid)
        }
        Expr::Paren(x) => validate_where(x, valid),
        Expr::Cmp(l, _, _) => prefix_col(l, valid, "invalid comparison expression"),
        Expr::Range(l, _, _, _) => prefix_col(l, valid, "invalid range expression"),
        Expr::Not(_) | Expr::Is => Err(bad("invalid where clause")),
    }
}

fn prefix_col(l: &mut Operand, valid: &dyn Fn(&str) -> bool, msg: &str) -> Result<(), QueryError> {
    let name = match l {
        // Go's ColName splits `qualifier.name`; validation uses the name only.
        Operand::Col(n, _) => n.rsplit('.').next().unwrap_or(n).to_owned(),
        _ => return Err(bad(msg)),
    };
    if !valid(&name) {
        return Err(bad(format!("invalid search attribute {name:?}")));
    }
    if !is_system_key(&name) {
        *l = Operand::Col(format!("Attr.{name}"), false);
    }
    Ok(())
}

// ---------------------------------------------------- elasticsql conversion

fn col_name(o: &Operand) -> Result<String, QueryError> {
    match o {
        Operand::Col(n, _) => Ok(n.clone()),
        _ => Err(bad(
            "elasticsql: invalid comparison expression, the left must be a column name",
        )),
    }
}

/// Column text as elasticsql emits it (backtick-wrapped if the validator
/// quoted it, because `fmt_col` quotes names containing '.').
fn es_col(o: &Operand) -> Result<String, QueryError> {
    match o {
        Operand::Col(n, _) => Ok(if n.contains('.') {
            format!("`{n}`")
        } else {
            n.clone()
        }),
        _ => col_name(o),
    }
}

fn val_text(o: &Operand) -> String {
    match o {
        Operand::Val(v, _) => v.clone(),
        Operand::Other(s) => s.clone(),
        Operand::Col(n, _) => n.clone(),
        Operand::Tuple(_) => String::new(),
    }
}

fn convert_expr(e: &Expr, parent: Option<&str>) -> Result<Value, QueryError> {
    match e {
        Expr::Paren(x) => convert_expr(x, parent),
        Expr::Not(_) => Err(bad("elasticsql: not expression currently not supported")),
        Expr::Is => Err(bad("elasticsql: is expression currently not supported")),
        Expr::And(l, r) | Expr::Or(l, r) => {
            let (key, me) = if matches!(e, Expr::And(..)) {
                ("must", "and")
            } else {
                ("should", "or")
            };
            let lv = convert_expr(l, Some(me))?;
            let rv = convert_expr(r, Some(me))?;
            let mut items = Vec::new();
            for v in [lv, rv] {
                // merge children of the same operator flat
                match flatten_bool(&v, key) {
                    Some(inner) => items.extend(inner),
                    None => items.push(v),
                }
            }
            let _ = parent;
            Ok(json!({"bool": { key: items }}))
        }
        Expr::Cmp(l, op, r) => {
            let c = es_col(l)?;
            if matches!(r, Operand::Col(..))
                && !matches!(r, Operand::Col(n, false) if n.eq_ignore_ascii_case("missing"))
            {
                return Err(bad(
                    "elasticsql: column name on the right side of compare operator is not supported",
                ));
            }
            let v = val_text(r);
            let mp = |c: &str, v: &str| json!({"match_phrase": { c: {"query": v}}});
            let rng = |c: &str, k: &str, v: &str| json!({"range": { c: { k: v }}});
            let node = match op.as_str() {
                "=" | "<=>" => {
                    if matches!(r, Operand::Col(n, false) if n.eq_ignore_ascii_case("missing")) {
                        json!({"missing": {"field": c}})
                    } else {
                        mp(&c, &v)
                    }
                }
                "!=" => json!({"bool": {"must_not": [mp(&c, &v)]}}),
                ">=" => rng(&c, "from", &v),
                "<=" => rng(&c, "to", &v),
                ">" => rng(&c, "gt", &v),
                "<" => rng(&c, "lt", &v),
                "in" | "not in" => {
                    let items: Vec<Value> = match r {
                        Operand::Tuple(t) => {
                            t.iter().map(|(x, _)| Value::String(x.clone())).collect()
                        }
                        _ => vec![],
                    };
                    let terms = json!({"terms": { c.clone(): items }});
                    if op == "in" {
                        terms
                    } else {
                        json!({"bool": {"must_not": [terms]}})
                    }
                }
                "wildcard" => {
                    let pat = format!("{}*", v.replace('%', "*").replace('_', "?"));
                    if v.trim().is_empty() {
                        json!({"match_all": {}})
                    } else {
                        json!({"wildcard": { c: {"value": pat, "case_insensitive": true}}})
                    }
                }
                "like" => mp(&c, &v.replace('%', "")),
                "not like" => json!({"bool": {"must_not": [mp(&c, &v.replace('%', ""))]}}),
                _ => return Err(bad(format!("elasticsql: {op} is not supported"))),
            };
            Ok(wrap_top(node, parent))
        }
        Expr::Range(l, _neg, a, b) => {
            let c = es_col(l)?;
            let node = json!({"range": { c: {"from": val_text(a), "to": val_text(b)}}});
            Ok(wrap_top(node, parent))
        }
    }
}

fn wrap_top(node: Value, parent: Option<&str>) -> Value {
    if parent.is_none() {
        json!({"bool": {"must": [node]}})
    } else {
        node
    }
}

fn flatten_bool(v: &Value, key: &str) -> Option<Vec<Value>> {
    let b = v.get("bool")?.as_object()?;
    if b.len() != 1 {
        return None;
    }
    b.get(key)?.as_array().cloned()
}

/// Output of the SQL->DSL step (before Cadence post-processing).
fn sql_to_dsl(clause: &str, page_size: i64) -> Result<Value, QueryError> {
    let parsed = parse_clause(clause).map_err(|_| bad("elasticsql: syntax error"))?;
    let query = match &parsed.where_ {
        None => json!({"bool": {"must": [{"match_all": {}}]}}),
        Some(e) => convert_expr(&preprocess_like(e), None)?,
    };
    let (from, size) = parsed.limit.unwrap_or((0, page_size));
    let mut dsl = Map::new();
    dsl.insert("query".into(), query);
    dsl.insert("from".into(), json!(from));
    dsl.insert("size".into(), json!(size));
    if !parsed.order.is_empty() {
        let mut s = Vec::new();
        for (o, d) in &parsed.order {
            s.push(json!({ es_col(o)?: d }));
        }
        dsl.insert("sort".into(), Value::Array(s));
    }
    Ok(Value::Object(dsl))
}

/// Port of `extractLikeClauses`/`replaceDummyWithWildcard`: `col LIKE 'pat'`
/// on an unquoted column becomes a case-insensitive wildcard query.
fn preprocess_like(e: &Expr) -> Expr {
    match e {
        Expr::And(l, r) => Expr::And(Box::new(preprocess_like(l)), Box::new(preprocess_like(r))),
        Expr::Or(l, r) => Expr::Or(Box::new(preprocess_like(l)), Box::new(preprocess_like(r))),
        Expr::Paren(x) => Expr::Paren(Box::new(preprocess_like(x))),
        Expr::Cmp(Operand::Col(n, false), op, Operand::Val(v, true))
            if op == "like" && !n.contains('.') && !v.is_empty() && !v.contains('\'') =>
        {
            // marker: wildcard comparison, resolved in convert via op "wildcard"
            Expr::Cmp(
                Operand::Col(n.clone(), false),
                "wildcard".into(),
                Operand::Val(v.clone(), true),
            )
        }
        o => o.clone(),
    }
}

// ----------------------------------------------------------- post-processing

const TIME_KEYS: [&str; 4] = ["StartTime", "CloseTime", "ExecutionTime", "UpdateTime"];
const RANGE_KEYS: [&str; 5] = ["from", "to", "gt", "lt", "query"];

fn close_status_from_str(s: &str) -> Option<i64> {
    Some(match s.to_ascii_uppercase().as_str() {
        "COMPLETED" => 0,
        "FAILED" => 1,
        "CANCELED" => 2,
        "TERMINATED" => 3,
        "CONTINUED_AS_NEW" => 4,
        "TIMED_OUT" => 5,
        other => other.parse::<i64>().ok()?,
    })
}

fn process_values(v: &mut Value) -> Result<(), QueryError> {
    match v {
        Value::Object(m) => {
            let keys: Vec<String> = m.keys().cloned().collect();
            for k in keys {
                let is_time = TIME_KEYS.contains(&k.as_str());
                let is_status = k == "CloseStatus";
                if (is_time || is_status) && m[&k].is_object() {
                    let inner = m.get_mut(&k).and_then(|x| x.as_object_mut()).unwrap();
                    for rk in RANGE_KEYS {
                        if let Some(Value::String(s)) = inner.get(rk).cloned() {
                            let nv = if is_time {
                                match chrono::DateTime::parse_from_rfc3339(&s) {
                                    Ok(t) => t.timestamp_nanos_opt().map(|n| n.to_string()),
                                    Err(_) => {
                                        if s.parse::<i64>().is_ok() {
                                            None
                                        } else {
                                            return Err(bad(format!(
                                                "Invalid query. Cannot parse time value: {s}"
                                            )));
                                        }
                                    }
                                }
                            } else {
                                match close_status_from_str(&s) {
                                    Some(n) => Some(n.to_string()),
                                    None => {
                                        return Err(bad(format!("Invalid CloseStatus value: {s}")))
                                    }
                                }
                            };
                            if let Some(n) = nv {
                                inner.insert(rk.to_string(), Value::String(n));
                            }
                        }
                    }
                } else if let Some(x) = m.get_mut(&k) {
                    process_values(x)?;
                }
            }
            Ok(())
        }
        Value::Array(a) => {
            for x in a {
                process_values(x)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn replace_node(v: &mut Value, target: &Value, with: &Value) {
    if v == target {
        *v = with.clone();
        return;
    }
    match v {
        Value::Object(m) => m.values_mut().for_each(|x| replace_node(x, target, with)),
        Value::Array(a) => a.iter_mut().for_each(|x| replace_node(x, target, with)),
        _ => {}
    }
}

fn add_must(dsl: &mut Value, extra: Value) {
    let old = dsl["query"]["bool"].take();
    dsl["query"]["bool"] = json!({"must": [extra, {"bool": old}]});
}

fn standard_processing(dsl: &mut Value, domain_id: &str) -> Result<(), QueryError> {
    for f in ["StartTime", "CloseTime"] {
        let missing = json!({"missing": {"field": f}});
        let with = json!({"bool": {"must_not": {"exists": {"field": f}}}});
        replace_node(&mut dsl["query"], &missing, &with);
    }
    let compact = serde_json::to_string(&dsl["query"]).unwrap_or_default();
    if compact.contains("{\"range\":{\"ExecutionTime\":") {
        add_must(dsl, json!({"range": {"ExecutionTime": {"gt": "0"}}}));
    }
    add_must(
        dsl,
        json!({"match_phrase": {"DomainID": {"query": domain_id}}}),
    );
    process_values(dsl)
}

// ----------------------------------------------------------------- tokens

/// Port of `ElasticVisibilityPageToken` (no omitempty, same field names).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct PageToken {
    #[serde(rename = "From")]
    pub from: i64,
    #[serde(rename = "SortValue")]
    pub sort_value: Value,
    #[serde(rename = "TieBreaker")]
    pub tie_breaker: String,
    #[serde(rename = "ScrollID")]
    pub scroll_id: String,
}

impl PageToken {
    pub fn decode(data: &[u8]) -> Result<PageToken, QueryError> {
        if data.is_empty() {
            return Ok(PageToken::default());
        }
        serde_json::from_slice(data)
            .map_err(|e| bad(format!("unable to deserialize page token. err: {e}")))
    }
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    pub fn should_search_after(&self) -> bool {
        !self.tie_breaker.is_empty()
    }
}

fn clean_dsl(s: &str) -> String {
    // strip backticks wrapped around `Attr.xxx`
    let mut out = String::with_capacity(s.len());
    let c: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < c.len() {
        if c[i] == '`' {
            if let Some(j) = c[i + 1..].iter().position(|x| *x == '`') {
                let inner: String = c[i + 1..i + 1 + j].iter().collect();
                if inner.starts_with("Attr.")
                    && inner[5..].chars().all(|x| x.is_alphanumeric() || x == '_')
                {
                    out.push_str(&inner);
                    i += j + 2;
                    continue;
                }
            }
        }
        out.push(c[i]);
        i += 1;
    }
    out
}

fn process_sort(
    dsl: &mut Value,
    token: &PageToken,
    attr_types: &dyn Fn(&str) -> Option<i32>,
) -> Result<(), QueryError> {
    let sort = dsl.get("sort").and_then(|s| s.as_array()).cloned();
    let (field, mut arr) = match sort {
        None => (
            "StartTime".to_string(),
            vec![json!({"StartTime": "desc"}), json!({"RunID": "desc"})],
        ),
        Some(a) if a.is_empty() => (
            "StartTime".to_string(),
            vec![json!({"StartTime": "desc"}), json!({"RunID": "desc"})],
        ),
        Some(a) => {
            if a.len() > 1 {
                return Err(bad("only one field can be used to sort"));
            }
            let f = a[0]
                .as_object()
                .and_then(|m| m.keys().next().cloned())
                .unwrap_or_default();
            let bare = f.trim_matches('`');
            let name = bare.strip_prefix("Attr.").unwrap_or(bare);
            if attr_types(name) == Some(0) {
                return Err(bad(
                    "Not able to sort by IndexedValueTypeString field, use IndexedValueTypeKeyword field",
                ));
            }
            let mut a = a;
            a.insert(1, json!({"RunID": "desc"}));
            (f, a)
        }
    };
    let _ = &mut arr;
    dsl["sort"] = Value::Array(arr);
    if token.should_search_after() {
        dsl["search_after"] = json!([token.sort_value, token.tie_breaker]);
        dsl.as_object_mut().map(|m| m.remove("from"));
    } else {
        dsl["from"] = json!(token.from);
    }
    let _ = field;
    Ok(())
}

/// `getESQueryDSL` (ListWorkflowExecutions): returns the serialised body.
pub fn dsl_for_list(
    query: &str,
    domain_id: &str,
    page_size: i64,
    token: &PageToken,
    attr_types: &dyn Fn(&str) -> Option<i32>,
) -> Result<String, QueryError> {
    let mut dsl = sql_to_dsl_checked(query, page_size)?;
    standard_processing(&mut dsl, domain_id)?;
    process_sort(&mut dsl, token, attr_types)?;
    Ok(clean_dsl(&serde_json::to_string(&dsl).unwrap_or_default()))
}

/// `getESQueryDSLForScan`: no sort, scroll based.
pub fn dsl_for_scan(query: &str, domain_id: &str, page_size: i64) -> Result<String, QueryError> {
    let mut dsl = sql_to_dsl_checked(query, page_size)?;
    standard_processing(&mut dsl, domain_id)?;
    if let Some(m) = dsl.as_object_mut() {
        m.remove("sort");
    }
    Ok(clean_dsl(&serde_json::to_string(&dsl).unwrap_or_default()))
}

/// `getESQueryDSLForCount`: only the query part.
pub fn dsl_for_count(query: &str, domain_id: &str) -> Result<String, QueryError> {
    let mut dsl = sql_to_dsl_checked(query, 1)?;
    standard_processing(&mut dsl, domain_id)?;
    if let Some(m) = dsl.as_object_mut() {
        m.remove("from");
        m.remove("size");
        m.remove("sort");
    }
    Ok(clean_dsl(&serde_json::to_string(&dsl).unwrap_or_default()))
}

fn sql_to_dsl_checked(query: &str, page_size: i64) -> Result<Value, QueryError> {
    sql_to_dsl(query.trim(), page_size).map_err(|e| match e {
        QueryError::BadRequest(m) => bad(format!("Error when parse query: {m}")),
        o => o,
    })
}

/// Body of `ListOpenWorkflowExecutions` / `ListClosed…` (`getSearchResult`).
#[allow(clippy::too_many_arguments)]
pub fn open_closed_body(
    domain_id: &str,
    is_open: bool,
    earliest_nanos: i64,
    latest_nanos: i64,
    page_size: i64,
    token: &PageToken,
    match_query: Option<(String, Value)>,
) -> String {
    let field = if is_open { "StartTime" } else { "CloseTime" };
    // Go: earliest-1µs .. latest+1µs, stringified, clamped on overflow.
    let lo = earliest_nanos.saturating_sub(1000);
    let hi = latest_nanos.saturating_add(1000);
    let mut must = vec![json!({"match": {"DomainID": {"query": domain_id}}})];
    if let Some((k, v)) = match_query {
        must.push(json!({"match": { k: {"query": v}}}));
    }
    let mut b = Map::new();
    let exists = json!({"exists": {"field": "CloseStatus"}});
    if is_open {
        b.insert("must_not".into(), exists);
    } else {
        must.push(exists);
    }
    b.insert(
        "must".into(),
        if must.len() == 1 {
            must.remove(0)
        } else {
            Value::Array(must)
        },
    );
    b.insert(
        "filter".into(),
        json!({"range": { field: {
            "from": lo.to_string(), "to": hi.to_string(),
            "include_lower": true, "include_upper": true}}}),
    );
    let mut dsl = Map::new();
    dsl.insert("query".into(), json!({"bool": Value::Object(b)}));
    dsl.insert("size".into(), json!(page_size));
    dsl.insert(
        "sort".into(),
        json!([{ field: {"order": "desc"}}, {"RunID": {"order": "desc"}}]),
    );
    if token.should_search_after() {
        dsl.insert(
            "search_after".into(),
            json!([token.sort_value, token.tie_breaker]),
        );
    } else {
        dsl.insert("from".into(), json!(token.from));
    }
    serde_json::to_string(&Value::Object(dsl)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs() -> HashMap<String, i32> {
        let mut m = HashMap::new();
        m.insert("CustomKeywordField".to_string(), 1);
        m.insert("WorkflowID".to_string(), 0);
        m.insert("StartTime".to_string(), 4);
        m
    }

    #[test]
    fn validate_prefixes_custom_keys() {
        let a = attrs();
        let q = validate_query(
            "WorkflowID = 'a' and CustomKeywordField = 'b' order by StartTime",
            &a,
            true,
        )
        .unwrap();
        assert_eq!(
            q,
            "WorkflowID = 'a' and `Attr.CustomKeywordField` = 'b' order by StartTime asc"
        );
    }

    #[test]
    fn validate_errors() {
        let a = attrs();
        assert_eq!(validate_query("a = ", &a, true), Err(bad("Invalid query.")));
        assert_eq!(
            validate_query("Nope = 'x'", &a, true),
            Err(bad("invalid search attribute \"Nope\""))
        );
        assert_eq!(
            validate_query("not WorkflowID = 'x'", &a, true),
            Err(bad("invalid where clause"))
        );
        assert_eq!(
            validate_query("order by Nope", &a, true),
            Err(bad("invalid order by attribute"))
        );
        assert_eq!(validate_query("", &a, true), Ok(String::new()));
        // validation disabled accepts unknown keys
        assert!(validate_query("Nope = 'x'", &a, false).is_ok());
    }

    #[test]
    fn dsl_simple_equality() {
        let a = attrs();
        let q = validate_query("WorkflowID = 'wf1'", &a, true).unwrap();
        let s = dsl_for_list(&q, "dom", 10, &PageToken::default(), &|_| None).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["size"], json!(10));
        assert_eq!(v["from"], json!(0));
        assert_eq!(
            v["query"]["bool"]["must"][0],
            json!({"match_phrase": {"DomainID": {"query": "dom"}}})
        );
        assert_eq!(
            v["query"]["bool"]["must"][1]["bool"]["must"][0],
            json!({"match_phrase": {"WorkflowID": {"query": "wf1"}}})
        );
        assert_eq!(v["sort"], json!([{"StartTime": "desc"}, {"RunID": "desc"}]));
    }

    #[test]
    fn dsl_custom_attr_strips_backticks_and_time_converts() {
        let a = attrs();
        let q = validate_query(
            "CustomKeywordField = 'x' and StartTime > '2021-01-01T00:00:00Z'",
            &a,
            true,
        )
        .unwrap();
        let s = dsl_for_count(&q, "d").unwrap();
        assert!(s.contains("\"Attr.CustomKeywordField\""), "{s}");
        assert!(!s.contains('`'));
        assert!(s.contains("\"gt\":\"1609459200000000000\""), "{s}");
        let v: Value = serde_json::from_str(&s).unwrap();
        assert!(v.get("size").is_none() && v.get("from").is_none());
    }

    #[test]
    fn dsl_close_status_and_missing() {
        let s = dsl_for_count("CloseStatus > 'FAILED' and CloseTime = missing", "d").unwrap();
        assert!(s.contains("\"CloseStatus\":{\"gt\":\"1\"}"), "{s}");
        assert!(
            s.contains("\"must_not\":{\"exists\":{\"field\":\"CloseTime\"}}"),
            "{s}"
        );
    }

    #[test]
    fn dsl_in_between_or() {
        let s = dsl_for_count(
            "WorkflowType in ('a', 'b') or HistoryLength between 1 and 5",
            "d",
        )
        .unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        let inner = &v["query"]["bool"]["must"][1]["bool"]["should"];
        assert_eq!(inner[0], json!({"terms": {"WorkflowType": ["a", "b"]}}));
        assert_eq!(
            inner[1],
            json!({"range": {"HistoryLength": {"from": "1", "to": "5"}}})
        );
    }

    #[test]
    fn like_becomes_wildcard() {
        let s = dsl_for_count("WorkflowType like 'ab_c%'", "d").unwrap();
        assert!(s.contains("\"wildcard\""), "{s}");
    }

    #[test]
    fn execution_time_adds_gt_zero() {
        let s = dsl_for_count("ExecutionTime > 5", "d").unwrap();
        assert!(
            s.contains("{\"range\":{\"ExecutionTime\":{\"gt\":\"0\"}}}"),
            "{s}"
        );
    }

    #[test]
    fn parse_error_message() {
        let e = dsl_for_count("WorkflowID = = 'x'", "d").unwrap_err();
        assert!(matches!(e, QueryError::BadRequest(m) if m.starts_with("Error when parse query")));
    }

    #[test]
    fn sort_user_field_and_search_after() {
        let tok = PageToken {
            from: 0,
            sort_value: json!(123),
            tie_breaker: "run".into(),
            scroll_id: String::new(),
        };
        let s = dsl_for_list("order by CloseTime desc", "d", 5, &tok, &|_| None).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["sort"], json!([{"CloseTime": "desc"}, {"RunID": "desc"}]));
        assert_eq!(v["search_after"], json!([123, "run"]));
        assert!(v.get("from").is_none());
        let e = dsl_for_list(
            "order by CloseTime desc, StartTime desc",
            "d",
            5,
            &PageToken::default(),
            &|_| None,
        );
        assert_eq!(e, Err(bad("only one field can be used to sort")));
    }

    #[test]
    fn sort_by_string_rejected() {
        let e = dsl_for_list(
            "order by Attr.S desc",
            "d",
            5,
            &PageToken::default(),
            &|n| {
                if n == "S" {
                    Some(0)
                } else {
                    None
                }
            },
        );
        assert!(
            matches!(e, Err(QueryError::BadRequest(m)) if m.contains("IndexedValueTypeString"))
        );
    }

    #[test]
    fn page_token_roundtrip_and_errors() {
        let t = PageToken {
            from: 7,
            ..Default::default()
        };
        assert_eq!(PageToken::decode(&t.encode()).unwrap(), t);
        assert!(
            matches!(PageToken::decode(b"{bad"), Err(QueryError::BadRequest(m)) if m.starts_with("unable to deserialize page token"))
        );
        assert_eq!(PageToken::decode(b"").unwrap(), PageToken::default());
    }

    #[test]
    fn open_closed_shapes() {
        let s = open_closed_body(
            "d",
            true,
            1_000_000,
            2_000_000,
            10,
            &PageToken::default(),
            None,
        );
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(
            v["query"]["bool"]["must"],
            json!({"match": {"DomainID": {"query": "d"}}})
        );
        assert_eq!(
            v["query"]["bool"]["must_not"],
            json!({"exists": {"field": "CloseStatus"}})
        );
        assert_eq!(
            v["query"]["bool"]["filter"]["range"]["StartTime"]["from"],
            json!("999000")
        );
        assert_eq!(
            v["query"]["bool"]["filter"]["range"]["StartTime"]["to"],
            json!("2001000")
        );
        let c = open_closed_body(
            "d",
            false,
            0,
            0,
            10,
            &PageToken::default(),
            Some(("WorkflowType".into(), json!("t"))),
        );
        let v: Value = serde_json::from_str(&c).unwrap();
        assert_eq!(v["query"]["bool"]["must"].as_array().unwrap().len(), 3);
        assert_eq!(v["sort"][0], json!({"CloseTime": {"order": "desc"}}));
    }

    /// Mirrors go/common/elasticsearch/validator TestValidateQuery.
    #[test]
    #[rustfmt::skip]
    fn go_validate_query_table() {
        let mut a: HashMap<String, i32> = cadence_api::search_attributes::default_indexed_keys()
            .into_keys()
            .map(|k| (k, 0))
            .collect();
        for k in ["Custom_Field", "Custom_0", "Custom.Field", "Custom-Field"] {
            a.insert(k.to_string(), 0);
        }
        let cases: &[(&str, Result<&str, &str>)] = &[
            ("", Ok("")),
            ("WorkflowID = 'wid'", Ok("WorkflowID = 'wid'")),
            ("CustomStringField = 'custom'", Ok("`Attr.CustomStringField` = 'custom'")),
            ("Invalid SQL", Err("Invalid query.")),
            ("InvalidWhereExpr", Err("invalid where clause")),
            ("WorkflowID = 'wid' and 1 < 2", Err("invalid comparison expression")),
            ("1 between 1 and 2 or WorkflowID = 'wid'", Err("invalid range expression")),
            ("Invalid = 'a' and 1 < 2", Err("invalid search attribute \"Invalid\"")),
            ("Invalid between 1 and 2 or WorkflowID = 'wid'", Err("invalid search attribute \"Invalid\"")),
            ("order by InvalidField desc", Err("invalid order by attribute")),
            ("order by 123", Err("invalid order by expression")),
            ("WorkflowID = 'wid'; SELECT * FROM important_table;", Err("Invalid query.")),
            ("WorkflowID = 'wid' and (RunID = 'rid' or 1 = 1)", Err("invalid comparison expression")),
            ("WorkflowID = 'wid' union select * from dummy", Err("Invalid select query.")),
            ("Custom_Field = 'value'", Ok("`Attr.Custom_Field` = 'value'")),
            ("Custom_0 = 'value'", Ok("`Attr.Custom_0` = 'value'")),
            ("Custom.Field = 'value'", Err("invalid search attribute \"Field\"")),
            ("Custom-Field = 'value'", Err("invalid comparison expression")),
            ("CustomStringField not like 'value'", Ok("`Attr.CustomStringField` not like 'value'")),
        ];
        for (q, want) in cases {
            let got = validate_query(q, &a, true);
            match want {
                Ok(v) => assert_eq!(got, Ok(v.to_string()), "{q}"),
                Err(m) => assert_eq!(got, Err(bad(*m)), "{q}"),
            }
        }
    }
}
