"""Small deterministic book activity with deliberately slow top-ten turnover."""
BOOKS = 20
TICKS_PER_SECOND = 2
MEMBERSHIP_SECONDS = 35


class Phases:
    """Advance by elapsed time, with no catch-up burst after a slow commit."""
    def __init__(self, phase, now):
        self.phase = phase
        self.deadline = now + MEMBERSHIP_SECONDS

    def current(self, now):
        if now >= self.deadline:
            self.phase += 1
            self.deadline = now + MEMBERSHIP_SECONDS
        return self.phase


def total(book, phase):
    challenger = 11 + (phase // 3) % 10
    if book == challenger:
        if phase % 3 == 1:
            return 9450  # Outside entry; book ten leaves the cutoff.
        if phase > 0 and phase % 3 == 0:
            return 9000  # Tie with book ten, which wins by smaller book ID.
    return 10000 - book * 100


def initial_sql():
    values = ','.join(f'({book*10+offset},{book},{total(book, 0) if offset == 1 else 0})'
                      for book in range(1, BOOKS + 1) for offset in (1, 2))
    return 'INSERT INTO public.demo_positions(id,book_id,quantity) VALUES' + values + ';'


def changes_sql(tick, phase, previous_phase):
    statements = []
    if phase != previous_phase:
        cases = ' '.join(f'WHEN {book} THEN {total(book, phase)}' for book in range(1, BOOKS + 1))
        statements.append(f'UPDATE public.demo_positions SET quantity=CASE book_id {cases} END WHERE book_id<=20 AND id%10=1')
        if phase % 2:
            statements.append('INSERT INTO public.demo_positions VALUES(211,21,500),(212,21,NULL) ON CONFLICT(id) DO UPDATE SET quantity=EXCLUDED.quantity')
        else:
            statements.append('DELETE FROM public.demo_positions WHERE book_id=21')
    book = 1 + tick % BOOKS
    # Only the leading eight books get a tiny units pulse. The cutoff and its
    # tie remain stable between explicit membership phases.
    units = total(book, phase) + ((tick % 3) - 1 if book <= 8 else 0)
    statements.append(f'UPDATE public.demo_positions SET quantity={units} WHERE id={book*10+1}')
    if (tick // BOOKS) % 2 == 0:
        statements.append(f'INSERT INTO public.demo_positions VALUES({book*10+3},{book},NULL) ON CONFLICT(id) DO NOTHING')
    else:
        statements.append(f'DELETE FROM public.demo_positions WHERE id={book*10+3}')
    return ';'.join(statements) + ';', phase
