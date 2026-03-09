"""Transform Budget Bakers data into Cashew format.

Handles category mapping (BB flat → Cashew parent + subcategories),
label → budget conversion, and record → transaction mapping.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.models import (
    BBAccount,
    BBBudget,
    BBCategory,
    BBData,
    BBGoal,
    BBLabel,
    BBRecord,
    BudgetReoccurrence,
    CashewBudget,
    CashewCategory,
    CashewData,
    CashewObjective,
    CashewTransaction,
    CashewWallet,
)

# ── Cashew default categories (matching defaultCategories.dart) ──
# PKs 1-11 match the app's hardcoded defaults

CASHEW_DEFAULTS: dict[str, dict] = {
    "1": {"name": "Dining", "colour": "#607D8B", "iconName": "cutlery.png", "order": 0},
    "2": {"name": "Groceries", "colour": "#4CAF50", "iconName": "groceries.png", "order": 1},
    "3": {"name": "Shopping", "colour": "#E91E63", "iconName": "shopping.png", "order": 2},
    "4": {"name": "Transit", "colour": "#FFEB3B", "iconName": "tram.png", "order": 3},
    "5": {"name": "Entertainment", "colour": "#2196F3", "iconName": "popcorn.png", "order": 4},
    "6": {"name": "Bills & Fees", "colour": "#4CAF50", "iconName": "bills.png", "order": 5},
    "7": {"name": "Gifts", "colour": "#F44336", "iconName": "gift.png", "order": 6},
    "8": {"name": "Beauty", "colour": "#9C27B0", "iconName": "flower.png", "order": 8},
    "9": {"name": "Work", "colour": "#795548", "iconName": "briefcase.png", "order": 9},
    "10": {"name": "Travel", "colour": "#FF9800", "iconName": "plane.png", "order": 10},
    "11": {"name": "Income", "colour": "#CE93D8", "iconName": "coin.png", "order": 11, "income": True},
}

# ── BB category name → (Cashew parent PK, subcategory name or None) ──
# None for subcategory name means map directly to parent (no subcategory)

CATEGORY_MAP: dict[str, tuple[str, str | None]] = {
    # Dining (parent: 1)
    "Restaurant, fast-food": ("1", "Restaurant/Fast-food"),
    "Ice cream, cafe": ("1", "Ice cream/Cafe"),
    "Bar, cafe": ("1", "Bar/Cafe"),
    # Groceries (parent: 2)
    "Groceries": ("2", None),
    "Food & Drinks": ("2", "Food & Drinks"),
    "Alcohol, tobacco": ("2", "Alcohol/Tobacco"),
    # Shopping (parent: 3)
    "Shopping": ("3", None),
    "Clothing & shoes": ("3", "Clothing & Shoes"),
    "Electronics, accessories": ("3", "Electronics"),
    "Stationery, tools": ("3", "Stationery/Tools"),
    "Kids": ("3", "Kids"),
    "Pets, animals": ("3", "Pets"),
    "Home, garden": ("3", "Home & Garden"),
    "Software, apps, games": ("3", "Software/Apps"),
    # Transit (parent: 4)
    "Fuel": ("4", "Fuel"),
    "Taxi": ("4", "Taxi"),
    "Public transport": ("4", "Public Transport"),
    "Vehicle maintenance": ("4", "Vehicle Maintenance"),
    "Vehicle insurance": ("4", "Vehicle Insurance"),
    "Parking": ("4", "Parking"),
    "Transportation": ("4", None),
    "Long distance": ("4", "Long Distance"),
    # Entertainment (parent: 5)
    "Culture, sport events": ("5", "Culture/Sport Events"),
    "Hobbies": ("5", "Hobbies"),
    "Sport, fitness": ("5", "Sport/Fitness"),
    "Books, audio, subscriptions": ("5", "Subscriptions"),
    "Education, development": ("5", "Education"),
    "Leisure": ("5", "Leisure"),
    "Life events": ("5", "Life Events"),
    "Life Events": ("5", "Life Events"),
    "Games": ("5", "Games"),
    # Bills & Fees (parent: 6)
    "Charges, Fees": ("6", "Charges/Fees"),
    "Loan, interests": ("6", "Loan/Interests"),
    "Phone, cell phone": ("6", "Phone"),
    "Internet": ("6", "Internet"),
    "Insurance": ("6", "Insurance"),
    "Rent": ("6", "Rent"),
    "Energy, utilities": ("6", "Utilities"),
    "Taxes": ("6", "Taxes"),
    "Financial expenses": ("6", "Financial Expenses"),
    "Fines": ("6", "Fines"),
    "Postal services": ("6", "Postal Services"),
    "Mortgage": ("6", "Mortgage"),
    "Debt": ("6", "Debt"),
    # Gifts (parent: 7)
    "Gifts, joy": ("7", None),
    "Charity, gifts": ("7", None),
    "Gifts": ("7", None),
    # Beauty & Health (parent: 8)
    "Drug-store, chemist": ("8", "Drug Store"),
    "Health care, doctor": ("8", "Health Care"),
    "Health and beauty": ("8", None),
    "Active sport, fitness": ("8", "Fitness"),
    # Work (parent: 9)
    "Work": ("9", None),
    # Travel (parent: 10)
    "Travel": ("10", None),
    "Holiday, trips, hotels": ("10", "Hotels/Trips"),
    # Income (parent: 11)
    "Salary, income": ("11", "Salary"),
    "Wage, invoices": ("11", "Wage"),
    "Rental income": ("11", "Rental Income"),
    "Interest, dividends": ("11", "Interest/Dividends"),
    "Interests, dividends": ("11", "Interest/Dividends"),
    "Refunds (tax, purchase)": ("11", "Refunds"),
    "Lending, renting": ("11", "Lending"),
    "Checks, coupons": ("11", "Checks/Coupons"),
    "Lottery, gambling": ("11", "Lottery"),
    "Sale": ("11", "Sale"),
    "Child Support": ("11", "Child Support"),
    "Investments": ("11", "Investments"),
    "Income": ("11", None),
    # Additional BB category name variants found in records
    "Clothes & shoes": ("3", "Clothing & Shoes"),
    "TV, Streaming": ("5", "TV/Streaming"),
    "Transfer, withdraw": ("6", "Transfer/Withdraw"),
    "Haircut": ("8", "Haircut"),
    "Gifts, Salam": ("7", "Gifts/Salam"),
    "Housing": ("6", "Housing"),
    "Sports": ("5", "Sports"),
    "Jewels, accessories": ("3", "Jewels/Accessories"),
    "Business trips": ("10", "Business Trips"),
    "Life & Entertainment": ("5", "Life & Entertainment"),
    "Good Deeds": ("7", "Good Deeds"),
    "Services": ("6", "Services"),
    "Maintenance, repairs": ("6", "Maintenance/Repairs"),
    "Savings": ("6", "Savings"),
    # Misc — map to closest match
    "Unknown": ("3", "Unknown"),
    "Missing": ("3", "Missing"),
}

# Label name → meaning for budget creation
LABEL_MEANINGS: dict[str, str] = {
    "🏢": "Office",
    "💳": "Credit Card",  # Will be dropped, info in notes
    "🏡": "Home",
    "👬": "Friends",
    "💰": "Wajebaat",
    "🧑\u200d🦱": "Personal",
    "👫": "Couple",
    "🪙": "Investments",
    "🇮🇶": "Iraq",
    "🕋": "Umrah",
    "🏔️": "Trips",
    "☪️": "Good Deeds",
}

SKIP_LABELS = {"💳"}  # Labels to drop (preserved in notes instead)


def _merge_categories(
    api_categories: list[BBCategory],
    records: list[BBRecord],
) -> list[BBCategory]:
    """Merge categories from the API response with categories embedded in records.

    The BB /categories endpoint may not return all categories (pagination/limits),
    but each record embeds its category info. This collects all unique categories.
    """
    seen_ids: set[str] = set()
    merged: list[BBCategory] = []

    for cat in api_categories:
        if cat.id not in seen_ids:
            seen_ids.add(cat.id)
            merged.append(cat)

    for record in records:
        cat_ref = record.category
        if cat_ref.id not in seen_ids:
            seen_ids.add(cat_ref.id)
            merged.append(BBCategory(
                id=cat_ref.id,
                name=cat_ref.name or "Unknown",
                color=cat_ref.color or "#000000",
                envelopeId=cat_ref.envelopeId,
            ))

    return merged


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stable_uuid(namespace: str, key: str) -> str:
    """Generate a deterministic UUID from a namespace and key."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"wallet-to-cashew.{namespace}.{key}"))


def _parse_dt(s: str | None) -> datetime:
    """Parse various datetime string formats from BB API."""
    if not s:
        return _now()
    # Try ISO format first
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    # Fallback: try fromisoformat
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return _now()


def _bb_reoccurrence_to_cashew(bb_type: str | None) -> int | None:
    """Map BB budget type to Cashew BudgetReoccurrence."""
    mapping = {
        "BUDGET_INTERVAL_MONTH": BudgetReoccurrence.MONTHLY.value,
        "BUDGET_INTERVAL_WEEK": BudgetReoccurrence.WEEKLY.value,
        "BUDGET_INTERVAL_YEAR": BudgetReoccurrence.YEARLY.value,
        "BUDGET_INTERVAL_DAY": BudgetReoccurrence.DAILY.value,
    }
    return mapping.get(bb_type or "", None)


def transform(bb_data: BBData) -> CashewData:
    """Transform Budget Bakers data into Cashew format."""
    print("\nTransforming data...")

    # Build lookup maps
    bb_category_map: dict[str, BBCategory] = {c.id: c for c in bb_data.categories}
    bb_account_map: dict[str, BBAccount] = {a.id: a for a in bb_data.accounts}
    bb_label_map: dict[str, BBLabel] = {l.id: l for l in bb_data.labels}

    # ── 1. Map accounts → wallets ──
    wallets = _transform_accounts(bb_data.accounts)
    wallet_id_map = {acc.id: wallets[i].walletPk for i, acc in enumerate(bb_data.accounts)}
    print(f"  Wallets: {len(wallets)}")

    # ── 2. Map categories → categories with subcategories ──
    # Merge categories from API response AND from record-embedded categories
    all_bb_categories = _merge_categories(bb_data.categories, bb_data.records)
    categories, category_id_map = _transform_categories(all_bb_categories)
    print(f"  Categories: {len(categories)} (parents + subcategories)")

    # ── 3. Map goals → objectives ──
    objectives = _transform_goals(bb_data.goals)
    print(f"  Objectives: {len(objectives)}")

    # ── 4. Map BB budgets → Cashew budgets ──
    budgets = _transform_budgets(bb_data.budgets, category_id_map, wallet_id_map)

    # ── 5. Map labels → additional Cashew budgets (addedTransactionsOnly) ──
    label_budgets, label_budget_map = _transform_labels_to_budgets(bb_data.labels)
    budgets.extend(label_budgets)
    print(f"  Budgets: {len(budgets)} ({len(label_budgets)} from labels)")

    # ── 6. Map records → transactions ──
    transactions = _transform_records(
        bb_data.records,
        category_id_map,
        wallet_id_map,
        label_budget_map,
    )
    print(f"  Transactions: {len(transactions)}")

    # Save category mapping for review
    _save_category_mapping(bb_data.categories, category_id_map)

    return CashewData(
        wallets=wallets,
        categories=categories,
        transactions=transactions,
        budgets=budgets,
        objectives=objectives,
    )


def _transform_accounts(accounts: list[BBAccount]) -> list[CashewWallet]:
    """Map BB accounts to Cashew wallets."""
    wallets: list[CashewWallet] = []
    for i, acc in enumerate(accounts):
        currency = None
        if acc.initialBalance:
            currency = acc.initialBalance.currencyCode
        elif acc.initialBaseBalance:
            currency = acc.initialBaseBalance.currencyCode

        wallets.append(CashewWallet(
            walletPk=_stable_uuid("wallet", acc.id),
            name=acc.name,
            colour=acc.color,
            dateCreated=_parse_dt(acc.createdAt),
            dateTimeModified=_parse_dt(acc.updatedAt or acc.createdAt),
            order=i,
            currency=currency,
        ))
    return wallets


def _transform_categories(
    bb_categories: list[BBCategory],
) -> tuple[list[CashewCategory], dict[str, str]]:
    """Map BB categories to Cashew categories with subcategories.

    Returns (cashew_categories, bb_category_id -> cashew_category_pk mapping).
    """
    now = _now()
    categories: list[CashewCategory] = []
    id_map: dict[str, str] = {}  # BB category ID → Cashew category PK

    # First, create the Cashew default parent categories
    for pk, defaults in CASHEW_DEFAULTS.items():
        categories.append(CashewCategory(
            categoryPk=pk,
            name=defaults["name"],
            colour=defaults.get("colour"),
            iconName=defaults.get("iconName"),
            dateCreated=now,
            dateTimeModified=now,
            order=defaults.get("order", 0),
            income=defaults.get("income", False),
        ))

    # Track created subcategories to avoid duplicates
    created_subcats: dict[str, str] = {}  # (parent_pk, subcat_name) → cashew_pk
    next_order = len(CASHEW_DEFAULTS) + 1
    new_top_level_order = 100  # Start new top-level categories at high order

    for bb_cat in bb_categories:
        cat_name = bb_cat.name.strip()
        mapping = CATEGORY_MAP.get(cat_name)

        if mapping:
            parent_pk, subcat_name = mapping

            if subcat_name is None:
                # Direct match to parent — map BB ID to Cashew parent PK
                id_map[bb_cat.id] = parent_pk
            else:
                # Create subcategory under parent
                subcat_key = (parent_pk, subcat_name)
                if subcat_key in created_subcats:
                    id_map[bb_cat.id] = created_subcats[subcat_key]
                else:
                    subcat_pk = _stable_uuid("category", f"{parent_pk}.{subcat_name}")
                    parent_income = CASHEW_DEFAULTS.get(parent_pk, {}).get("income", False)
                    categories.append(CashewCategory(
                        categoryPk=subcat_pk,
                        name=subcat_name,
                        colour=bb_cat.color,
                        dateCreated=_parse_dt(bb_cat.createdAt),
                        dateTimeModified=_parse_dt(bb_cat.updatedAt or bb_cat.createdAt),
                        order=next_order,
                        income=parent_income,
                        mainCategoryPk=parent_pk,
                    ))
                    created_subcats[subcat_key] = subcat_pk
                    id_map[bb_cat.id] = subcat_pk
                    next_order += 1
        else:
            # No mapping found — create as new top-level category
            new_pk = _stable_uuid("category", f"new.{cat_name}")
            is_income = bb_cat.name.lower() in (
                "salary", "income", "wage", "refund", "interest",
            )
            categories.append(CashewCategory(
                categoryPk=new_pk,
                name=cat_name,
                colour=bb_cat.color,
                iconName=bb_cat.iconName,
                dateCreated=_parse_dt(bb_cat.createdAt),
                dateTimeModified=_parse_dt(bb_cat.updatedAt or bb_cat.createdAt),
                order=new_top_level_order,
                income=is_income,
            ))
            id_map[bb_cat.id] = new_pk
            new_top_level_order += 1
            print(f"    New top-level category: '{cat_name}' (no Cashew default match)")

    return categories, id_map


def _transform_goals(goals: list[BBGoal]) -> list[CashewObjective]:
    """Map BB goals to Cashew objectives."""
    objectives: list[CashewObjective] = []
    for i, goal in enumerate(goals):
        objectives.append(CashewObjective(
            objectivePk=_stable_uuid("objective", goal.id),
            type=0,  # goal
            name=goal.name,
            amount=float(goal.targetAmount),
            order=i,
            colour=goal.color,
            dateCreated=_parse_dt(goal.createdAt),
            endDate=_parse_dt(goal.desiredDate) if goal.desiredDate else None,
            dateTimeModified=_parse_dt(goal.updatedAt or goal.createdAt),
            iconName=goal.iconName,
            income=False,
            pinned=True,
            archived=goal.state == "completed",
        ))
    return objectives


def _transform_budgets(
    bb_budgets: list[BBBudget],
    category_id_map: dict[str, str],
    wallet_id_map: dict[str, str],
) -> list[CashewBudget]:
    """Map BB budgets to Cashew budgets."""
    budgets: list[CashewBudget] = []
    for i, bb_budget in enumerate(bb_budgets):
        # Map category IDs
        cashew_cat_fks = [
            category_id_map[cid]
            for cid in bb_budget.categoryIds
            if cid in category_id_map
        ]

        # Map account IDs
        cashew_wallet_fks = [
            wallet_id_map[aid]
            for aid in bb_budget.accountIds
            if aid in wallet_id_map
        ]

        start = _parse_dt(bb_budget.startDate)
        end = _parse_dt(bb_budget.endDate) if bb_budget.endDate else start

        budgets.append(CashewBudget(
            budgetPk=_stable_uuid("budget", bb_budget.id),
            name=bb_budget.name,
            amount=float(bb_budget.amount),
            startDate=start,
            endDate=end,
            walletFks=json.dumps(cashew_wallet_fks) if cashew_wallet_fks else None,
            categoryFks=json.dumps(cashew_cat_fks) if cashew_cat_fks else None,
            income=False,
            addedTransactionsOnly=False,
            periodLength=1,
            reoccurrence=_bb_reoccurrence_to_cashew(bb_budget.type),
            dateCreated=_parse_dt(bb_budget.createdAt),
            dateTimeModified=_parse_dt(bb_budget.updatedAt or bb_budget.createdAt),
            order=i,
        ))
    return budgets


def _transform_labels_to_budgets(
    labels: list[BBLabel],
) -> tuple[list[CashewBudget], dict[str, str]]:
    """Convert BB labels into Cashew budgets with addedTransactionsOnly=True.

    Returns (budgets, label_id -> budget_pk mapping).
    """
    budgets: list[CashewBudget] = []
    label_budget_map: dict[str, str] = {}
    now = _now()

    for i, label in enumerate(labels):
        if label.name in SKIP_LABELS:
            continue

        meaning = LABEL_MEANINGS.get(label.name, label.name)
        budget_pk = _stable_uuid("label-budget", label.id)

        budgets.append(CashewBudget(
            budgetPk=budget_pk,
            name=f"{label.name} {meaning}",
            amount=0,  # No predefined limit — tracking only
            colour=label.color,
            startDate=datetime(2024, 1, 1, tzinfo=timezone.utc),
            endDate=datetime(2030, 12, 31, tzinfo=timezone.utc),
            addedTransactionsOnly=True,
            periodLength=1,
            reoccurrence=BudgetReoccurrence.MONTHLY.value,
            dateCreated=_parse_dt(label.createdAt),
            dateTimeModified=_parse_dt(label.updatedAt or label.createdAt),
            order=100 + i,
        ))
        label_budget_map[label.id] = budget_pk

    return budgets, label_budget_map


def _transform_records(
    records: list[BBRecord],
    category_id_map: dict[str, str],
    wallet_id_map: dict[str, str],
    label_budget_map: dict[str, str],
) -> list[CashewTransaction]:
    """Map BB records to Cashew transactions."""
    transactions: list[CashewTransaction] = []
    unmapped_categories: set[str] = set()

    for record in records:
        # Map category
        category_pk = category_id_map.get(record.category.id)
        if not category_pk:
            cat_name = record.category.name or record.category.id
            if cat_name not in unmapped_categories:
                unmapped_categories.add(cat_name)
                print(f"    Warning: unmapped category '{cat_name}', using parent")
            # Fallback: try to find a reasonable parent
            category_pk = "3"  # Default to Shopping

        # Map wallet
        wallet_pk = wallet_id_map.get(record.accountId, "0")

        # Build note with label info
        note_parts: list[str] = []
        if record.note:
            note_parts.append(record.note)

        # Preserve 💳 label and payment type info in notes
        credit_card_label = any(l.name in SKIP_LABELS for l in record.labels)
        if credit_card_label:
            note_parts.append("[💳 Card Payment]")
        if record.paymentType and record.paymentType not in ("cash", "undefined"):
            note_parts.append(f"[Payment: {record.paymentType}]")

        note = " | ".join(note_parts) if note_parts else ""

        # Determine budget exclusions
        # For addedTransactionsOnly budgets, transactions NOT tagged with
        # the label should exclude themselves from that budget
        all_label_budget_pks = set(label_budget_map.values())
        record_label_budget_pks = {
            label_budget_map[l.id]
            for l in record.labels
            if l.id in label_budget_map
        }
        exclude_budget_pks = list(all_label_budget_pks - record_label_budget_pks)

        is_income = record.recordType == "income"

        # Transaction name: use payee/payer or note or category name
        name = record.payee or record.payer or ""
        if not name and record.note:
            name = record.note[:50]

        transactions.append(CashewTransaction(
            transactionPk=_stable_uuid("transaction", record.id),
            name=name,
            amount=abs(record.amount.value),
            note=note,
            categoryFk=category_pk,
            walletFk=wallet_pk,
            dateCreated=_parse_dt(record.recordDate),
            dateTimeModified=_parse_dt(record.updatedAt or record.recordDate),
            income=is_income,
            paid=record.recordState == "cleared",
            budgetFksExclude=json.dumps(exclude_budget_pks) if exclude_budget_pks else None,
        ))

    if unmapped_categories:
        print(f"    Total unmapped categories: {len(unmapped_categories)}")

    return transactions


def _save_category_mapping(
    bb_categories: list[BBCategory],
    id_map: dict[str, str],
) -> None:
    """Save category mapping to a JSON file for review."""
    mapping_data = []
    for cat in bb_categories:
        cashew_pk = id_map.get(cat.id, "UNMAPPED")
        mapped_to = CATEGORY_MAP.get(cat.name.strip())
        mapping_data.append({
            "bb_id": cat.id,
            "bb_name": cat.name,
            "cashew_pk": cashew_pk,
            "mapped_to_parent": mapped_to[0] if mapped_to else None,
            "subcategory_name": mapped_to[1] if mapped_to else None,
            "status": "mapped" if mapped_to else "new_top_level",
        })

    path = Path("data/raw/category_mapping.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(mapping_data, f, indent=2, ensure_ascii=False)
    print(f"  Category mapping saved to {path}")


if __name__ == "__main__":
    from src.extract import load_from_raw

    bb_data = load_from_raw()
    cashew_data = transform(bb_data)
    print(f"\nTransformation complete. Ready for SQLite generation.")
