"""Pydantic models for Budget Bakers and Cashew data schemas."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Budget Bakers (Source) Models ──


class BBAmount(BaseModel):
    value: float
    currencyCode: str = "PKR"


class BBCategoryRef(BaseModel):
    id: str
    color: str | None = None
    name: str | None = None
    envelopeId: int | None = None


class BBLabel(BaseModel):
    id: str
    name: str
    color: str = "#000000"
    archived: bool = False
    createdAt: str | None = None
    updatedAt: str | None = None


class BBPhoto(BaseModel):
    createdAt: str | None = None
    temporaryUrl: str | None = None


class BBRecord(BaseModel):
    id: str
    accountId: str
    note: str | None = None
    payee: str | None = None
    payer: str | None = None
    amount: BBAmount
    baseAmount: BBAmount | None = None
    recordDate: str
    category: BBCategoryRef
    recordState: str | None = None
    recordType: str  # "expense" or "income"
    paymentType: str | None = None
    labels: list[BBLabel] = Field(default_factory=list)
    photos: list[BBPhoto] = Field(default_factory=list)
    createdAt: str | None = None
    updatedAt: str | None = None


class BBCategory(BaseModel):
    id: str
    color: str = "#000000"
    name: str
    envelopeId: int | None = None
    createdAt: str | None = None
    updatedAt: str | None = None
    customCategory: bool = False
    customColor: bool = False
    customName: bool = False
    iconName: str | None = None


class BBRecordStats(BaseModel):
    recordCount: int = 0
    recordDate: dict[str, str] | None = None
    createdAt: dict[str, str] | None = None


class BBAccount(BaseModel):
    id: str
    name: str
    color: str = "#000000"
    accountType: str = "General"
    archived: bool = False
    createdAt: str | None = None
    updatedAt: str | None = None
    initialBalance: BBAmount | None = None
    initialBaseBalance: BBAmount | None = None
    excludeFromStats: bool = False
    recordStats: BBRecordStats | None = None


class BBBudgetLabel(BaseModel):
    id: str
    name: str
    color: str = "#000000"
    archived: bool = False


class BBBudget(BaseModel):
    id: str
    name: str
    amount: str  # stored as string
    currencyCode: str = "PKR"
    type: str | None = None  # "BUDGET_INTERVAL_MONTH", "BUDGET_INTERVAL_WEEK"
    startDate: str | None = None
    endDate: str | None = None
    categoryIds: list[str] = Field(default_factory=list)
    accountIds: list[str] = Field(default_factory=list)
    labels: list[BBBudgetLabel] = Field(default_factory=list)
    createdAt: str | None = None
    updatedAt: str | None = None


class BBGoal(BaseModel):
    id: str
    name: str
    targetAmount: str  # stored as string
    initialAmount: str = "0"
    color: str = "#000000"
    iconName: str | None = None
    state: str = "active"
    stateUpdatedAt: str | None = None
    desiredDate: str | None = None
    createdAt: str | None = None
    updatedAt: str | None = None


class BBData(BaseModel):
    """Complete extracted Budget Bakers data."""

    accounts: list[BBAccount] = Field(default_factory=list)
    records: list[BBRecord] = Field(default_factory=list)
    categories: list[BBCategory] = Field(default_factory=list)
    labels: list[BBLabel] = Field(default_factory=list)
    budgets: list[BBBudget] = Field(default_factory=list)
    goals: list[BBGoal] = Field(default_factory=list)


# ── Cashew (Target) Models ──


class CashewWallet(BaseModel):
    walletPk: str
    name: str
    colour: str | None = None
    iconName: str | None = None
    dateCreated: datetime
    dateTimeModified: datetime
    order: int = 0
    currency: str | None = None
    currencyFormat: str | None = None
    decimals: int = 2
    homePageWidgetDisplay: str | None = None  # JSON string


class CashewCategory(BaseModel):
    categoryPk: str
    name: str
    colour: str | None = None
    iconName: str | None = None
    emojiIconName: str | None = None
    dateCreated: datetime
    dateTimeModified: datetime
    order: int = 0
    income: bool = False
    methodAdded: int | None = None
    mainCategoryPk: str | None = None  # parent category FK for subcategories


class CashewTransaction(BaseModel):
    transactionPk: str
    pairedTransactionFk: str | None = None
    name: str = ""
    amount: float = 0.0
    note: str = ""
    categoryFk: str
    subCategoryFk: str | None = None
    walletFk: str
    dateCreated: datetime
    dateTimeModified: datetime
    originalDateDue: datetime | None = None
    income: bool = False
    periodLength: int | None = None
    reoccurrence: int | None = None
    endDate: datetime | None = None
    upcomingTransactionNotification: bool = True
    type: int | None = None  # 0=upcoming, 1=subscription, 2=repetitive, 3=credit, 4=debt
    paid: bool = True
    createdAnotherFutureTransaction: bool = False
    skipPaid: bool = False
    methodAdded: int | None = None
    transactionOwnerEmail: str | None = None
    transactionOriginalOwnerEmail: str | None = None
    sharedKey: str | None = None
    sharedOldKey: str | None = None
    sharedStatus: int | None = None
    sharedDateUpdated: datetime | None = None
    sharedReferenceBudgetPk: str | None = None
    objectiveFk: str | None = None
    objectiveLoanFk: str | None = None
    budgetFksExclude: str | None = None  # JSON string


class BudgetReoccurrence(Enum):
    CUSTOM = 0
    DAILY = 1
    WEEKLY = 2
    MONTHLY = 3
    YEARLY = 4


class CashewBudget(BaseModel):
    budgetPk: str
    name: str
    amount: float = 0.0
    colour: str | None = None
    startDate: datetime
    endDate: datetime
    walletFks: str | None = None  # JSON string
    categoryFks: str | None = None  # JSON string
    categoryFksExclude: str | None = None  # JSON string
    income: bool = False
    archived: bool = False
    addedTransactionsOnly: bool = False
    periodLength: int = 1
    reoccurrence: int | None = None  # BudgetReoccurrence enum value
    dateCreated: datetime
    dateTimeModified: datetime
    pinned: bool = False
    order: int = 0
    walletFk: str = "0"
    budgetTransactionFilters: str | None = None  # JSON string
    memberTransactionFilters: str | None = None
    sharedKey: str | None = None
    sharedOwnerMember: int | None = None
    sharedDateUpdated: datetime | None = None
    sharedMembers: str | None = None
    sharedAllMembersEver: str | None = None
    isAbsoluteSpendingLimit: bool = False


class CashewObjective(BaseModel):
    objectivePk: str
    type: int = 0  # 0=goal, 1=loan
    name: str
    amount: float = 0.0
    order: int = 0
    colour: str | None = None
    dateCreated: datetime
    endDate: datetime | None = None
    dateTimeModified: datetime
    iconName: str | None = None
    emojiIconName: str | None = None
    income: bool = False
    pinned: bool = True
    archived: bool = False
    walletFk: str = "0"


class CashewCategoryBudgetLimit(BaseModel):
    categoryLimitPk: str
    categoryFk: str
    budgetFk: str
    amount: float = 0.0
    dateTimeModified: datetime
    walletFk: str | None = None


class CashewAssociatedTitle(BaseModel):
    associatedTitlePk: str
    categoryFk: str
    title: str
    dateCreated: datetime
    dateTimeModified: datetime
    order: int = 0
    isExactMatch: bool = False


class CashewData(BaseModel):
    """Complete Cashew database content ready for SQLite generation."""

    wallets: list[CashewWallet] = Field(default_factory=list)
    categories: list[CashewCategory] = Field(default_factory=list)
    transactions: list[CashewTransaction] = Field(default_factory=list)
    budgets: list[CashewBudget] = Field(default_factory=list)
    objectives: list[CashewObjective] = Field(default_factory=list)
    category_budget_limits: list[CashewCategoryBudgetLimit] = Field(default_factory=list)
    associated_titles: list[CashewAssociatedTitle] = Field(default_factory=list)
