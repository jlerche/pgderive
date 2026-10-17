"""Count semantic-inspection executions without recording SQL or parameters."""


def category(query):
    query = query.lower()
    if b'set search_path=pg_catalog' in query or b'set local search_path=pg_catalog' in query:
        return 'session_configuration'
    if b'with recursive native as' in query:
        return 'catalog'
    if b'pg_catalog.pg_proc p join pg_catalog.pg_language l' in query:
        return 'terminal_functions'
    if b'from pg_catalog.pg_collation c' in query:
        return 'terminal_collation'
    if b'pg_catalog.pg_timezone_abbrevs' in query:
        return 'terminal_calendar'
    return None


class BindingObserver:
    def __init__(self, classify=category):
        self.classify = classify
        self.statements = {}
        self.portals = {}

    def observe(self, kind, body):
        if kind == b'Q':
            return self.classify(body)
        if kind == b'P':
            statement, query, _ = body.split(b'\0', 2)
            self.statements[statement] = self.classify(query)
        elif kind == b'B':
            portal, statement, _ = body.split(b'\0', 2)
            self.portals[portal] = self.statements.get(statement)
        elif kind == b'E':
            portal, _ = body.split(b'\0', 1)
            return self.portals.get(portal)
        elif kind == b'C':
            name = body[1:-1]
            if body[:1] == b'S':
                self.statements.pop(name, None)
            else:
                self.portals.pop(name, None)
        return None
