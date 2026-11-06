def competition(pick):
    # Preserve provider competition names, including named cups.
    country=str(pick.get('country') or '').strip()
    league=str(pick.get('league') or '').strip()
    return ' · '.join(v for v in (country,league) if v) or 'Competizione non disponibile'

