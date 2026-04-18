from collections.abc import Iterable
from dataclasses import dataclass

from rules import RULES, predicate, rule

print(f"{RULES = }")
# ---------------------------------------------------------------------------
# DOMAIN MODEL
# ---------------------------------------------------------------------------


@dataclass
class User:
    is_admin: bool
    is_active: bool
    account_age: int
    is_banned: bool
    country: str
    credit_score: int
    has_manual_override: bool


# ---------------------------------------------------------------------------
# BUSINESS RULES (SIMPLY USE @predicate)
# ---------------------------------------------------------------------------


@rule
def is_admin(u: User) -> bool:
    return u.is_admin


@rule
def is_active(u: User) -> bool:
    return u.is_active


@rule
def is_banned(u: User) -> bool:
    return u.is_banned


@rule
def has_override(u: User) -> bool:
    return u.has_manual_override


@rule
def account_older_than(days: int, u: User) -> bool:
    return u.account_age > days


@rule
def from_country(countries: Iterable[str], u: User) -> bool:
    return u.country in countries


@rule
def credit_score_above(threshold: int, u: User) -> bool:
    return u.credit_score > threshold


# ---------------------------------------------------------------------------
# BUILD RULE IN PYTHON DSL
# ---------------------------------------------------------------------------

api_check = is_admin() | (
    is_active()
    & account_older_than(30)
    & ~is_banned()
    & from_country(["NL", "BE"])
    & (credit_score_above(650) | has_override())
)

print(f"{RULES = }")

users = [
    User(True, False, 1, False, "US", 100, False),
    User(False, True, 40, False, "NL", 700, False),
    User(False, True, 40, False, "BE", 500, True),
    User(False, True, 5, False, "NL", 900, False),
    User(False, False, 100, True, "NL", 900, False),
]

for u in users:
    print(f"{u = }, {api_check(u) = }")


# ---------------------------------------------------------------------------
# WITHOUT PASSING EXTRA PARAMS
# ---------------------------------------------------------------------------

@predicate
def is_admin(u: User) -> bool:
    return u.is_admin


@predicate
def is_active(u: User) -> bool:
    return u.is_active


@predicate
def is_banned(u: User) -> bool:
    return u.is_banned


@predicate
def has_override(u: User) -> bool:
    return u.has_manual_override


@predicate
def account_older_than(u: User) -> bool:
    return u.account_age > 30


@predicate
def from_country(u: User) -> bool:
    return u.country in ["NL", "BE"]


@predicate
def credit_score_above(u: User) -> bool:
    return u.credit_score > 650

api_check = is_admin | (
    is_active
    & account_older_than
    & ~is_banned
    & from_country
    & (credit_score_above | has_override)
)

for u in users:
    print(f"{u = }, {api_check(u) = }")
