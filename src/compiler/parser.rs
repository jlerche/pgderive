use anyhow::{Context, Result, ensure};
use sqlparser::{
    dialect::PostgreSqlDialect,
    keywords::Keyword,
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};

#[derive(Clone)]
pub(super) struct Name(pub(super) Vec<String>);
pub(super) struct Table {
    pub(super) name: Name,
    pub(super) alias: Option<String>,
}
pub(super) struct Parsed {
    pub(super) group: Name,
    pub(super) sum: Name,
    pub(super) left: Table,
    pub(super) right: Table,
    pub(super) keys: (Name, Name),
    pub(super) predicates: Vec<(Name, bool)>,
    pub(super) grouping: Name,
}
struct Parser {
    tokens: Vec<TokenWithSpan>,
    offset: usize,
}
pub(super) fn parse(sql: &str) -> Result<Parsed> {
    ensure!(sql.len() <= 16_384 && !sql.contains('\0'), "SQL exceeds size limit or contains NUL");
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()?
        .into_iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_)))
        .collect();
    let mut parser = Parser { tokens, offset: 0 };
    parser.query().with_context(|| parser.location())
}
impl Parser {
    fn query(&mut self) -> Result<Parsed> {
        self.keyword("select")?;
        let group = self.name()?;
        self.output_alias()?;
        self.symbol(&Token::Comma)?;
        self.keyword("count")?;
        self.symbol(&Token::LParen)?;
        self.symbol(&Token::Mul)?;
        self.symbol(&Token::RParen)?;
        self.output_alias()?;
        self.symbol(&Token::Comma)?;
        self.keyword("sum")?;
        self.symbol(&Token::LParen)?;
        let sum = self.name()?;
        self.symbol(&Token::RParen)?;
        self.output_alias()?;
        self.keyword("from")?;
        let left = self.table()?;
        self.take_keyword("inner");
        self.keyword("join")?;
        let right = self.table()?;
        self.keyword("on")?;
        let lhs = self.name()?;
        self.symbol(&Token::Eq)?;
        let rhs = self.name()?;
        let predicates = self.predicates()?;
        self.keyword("group")?;
        self.keyword("by")?;
        let grouping = self.name()?;
        self.take_symbol(&Token::SemiColon);
        ensure!(self.offset == self.tokens.len(), "unsupported trailing SQL clause or statement");
        Ok(Parsed { group, sum, left, right, keys: (lhs, rhs), predicates, grouping })
    }
    fn predicates(&mut self) -> Result<Vec<(Name, bool)>> {
        let mut predicates = Vec::new();
        if !self.take_keyword("where") {
            return Ok(predicates);
        }
        loop {
            let name = self.name()?;
            self.keyword("is")?;
            let not = self.take_keyword("not");
            self.keyword("null")?;
            predicates.push((name, not));
            if !self.take_keyword("and") {
                return Ok(predicates);
            }
        }
    }
    fn table(&mut self) -> Result<Table> {
        let name = self.name()?;
        ensure!(name.0.len() == 2, "FROM requires an explicit schema.table");
        let alias = if self.take_keyword("as") || self.is_identifier() {
            Some(self.identifier()?)
        } else {
            None
        };
        Ok(Table { name, alias })
    }
    fn name(&mut self) -> Result<Name> {
        let mut parts = vec![self.identifier()?];
        while self.take_symbol(&Token::Period) {
            ensure!(parts.len() < 3, "name has too many qualifiers");
            parts.push(self.identifier()?);
        }
        Ok(Name(parts))
    }
    fn output_alias(&mut self) -> Result<()> {
        if self.take_keyword("as") || self.is_identifier() {
            self.identifier()?;
        }
        Ok(())
    }
    fn is_identifier(&self) -> bool {
        matches!(self.tokens.get(self.offset).map(|t| &t.token),
            Some(Token::Word(word)) if word.quote_style == Some('"') || matches!(word.keyword, Keyword::NoKeyword | Keyword::SOURCE | Keyword::PUBLIC | Keyword::OTHER | Keyword::ID))
    }
    fn identifier(&mut self) -> Result<String> {
        ensure!(self.is_identifier(), "expected identifier (quote keyword names)");
        let Some(Token::Word(word)) = self.tokens.get(self.offset).map(|t| &t.token) else {
            anyhow::bail!("expected identifier");
        };
        let value = if word.quote_style.is_some() {
            word.value.clone()
        } else {
            ensure!(word.value.is_ascii(), "non-ASCII identifiers require double quotes");
            word.value.to_ascii_lowercase()
        };
        ensure!(!value.is_empty() && value.len() <= 63, "identifier length must be 1..=63 bytes");
        self.offset += 1;
        Ok(value)
    }
    fn keyword(&mut self, expected: &str) -> Result<()> {
        ensure!(self.take_keyword(expected), "expected {expected}; SQL shape is unsupported");
        Ok(())
    }
    fn take_keyword(&mut self, expected: &str) -> bool {
        let matched = matches!(self.tokens.get(self.offset).map(|t| &t.token),
            Some(Token::Word(word)) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected));
        if matched {
            self.offset += 1;
        }
        matched
    }
    fn symbol(&mut self, expected: &Token) -> Result<()> {
        ensure!(self.take_symbol(expected), "expected {expected}; expression is unsupported");
        Ok(())
    }
    fn take_symbol(&mut self, expected: &Token) -> bool {
        if self.tokens.get(self.offset).is_some_and(|token| &token.token == expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }
    fn location(&self) -> String {
        self.tokens.get(self.offset).map_or_else(
            || "SQL parse: end of input".into(),
            |token| format!("SQL parse: {}", token.span.start),
        )
    }
}
