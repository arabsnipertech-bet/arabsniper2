"""Telegram prediction slots. Times and kickoff bounds are local to Rome."""
WINDOWS = {
    '00:30': ('01:00', '04:30', 3),
    '04:30': ('06:00', '12:30', 3),
    '12:30': ('12:30', '17:00', 3),
    '17:30': ('17:30', '19:00', 1),
    '18:30': ('19:00', '20:00', 1),
    '20:00': ('20:00', '22:00', 1),
    '22:00': ('22:00', '23:59', 1),
}

EDITORIAL = {'12:45': 'promo', '16:45': 'education', '17:15': 'promo'}
CHANNEL_SLOTS = sorted((*WINDOWS, *EDITORIAL))
