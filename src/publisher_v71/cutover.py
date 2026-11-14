"""Production ownership of editorial delivery and the legacy public projection."""
ENABLED = True
TRIAL_PROVEN = True

PRIMARY = 'arabsnipertech-bet/arabsniper2'
GROWTH = 'arabsnipertech-bet/arabsniper-growth'
OWNER_FIELD = 'publisher_repository'
READY_KEY = 'growth-ready.json'


def owner(store):
    """Existing ledgers belong to the primary repository until the handoff."""
    repository = store.state['runtime'].get(OWNER_FIELD, PRIMARY)
    if repository not in (PRIMARY, GROWTH):
        raise ValueError('Invalid Publisher owner in the delivery ledger')
    return repository
