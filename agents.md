title Semantic Model Relationships

participant "MEMBER AGENT (Semantic Model)" as HUB

participant "CARDS PORTFOLIO MASTER" as CARDS
participant "DEPOSITS PORTFOLIO MASTER" as DEPOSITS
participant "CONSUMER LOANS PORTFOLIO MASTER" as LOANS
participant "APPLICATIONS & ORIGINATION MASTER" as APPS
participant "SERVICING & COLLECTIONS MASTER" as SERVICING

participant "CARDS TRANSACTION MODEL" as CARDS_TXN
participant "DEPOSITS TRANSACTION MODEL" as DEPOSITS_TXN
participant "DAILY DATA MODEL" as DAILY_DATA
participant "REWARDS LEDGER" as REWARDS
participant "CAMPAIGNS (Cards)" as CARDS_CAMP
participant "CAMPAIGNS (Deposits)" as DEPOSITS_CAMP
participant "CAMPAIGNS (Loans)" as LOANS_CAMP

participant "RISK & BUREAU SNAPSHOT" as RISK_BUREAU
participant "BRANCH LEVEL ORIGINATION" as BRANCH_ORIG

participant "ATOMIC - D SNAPS" as ATOMIC_D
participant "PERIODIC - M SNAPS" as PERIODIC_M
participant "AGENTS / SEMANTIC - MONTHLY SNAPS" as AGENT_MONTHLY

' Hub connections
HUB -> CARDS : member_accounts
HUB -> DEPOSITS : member_accounts
HUB -> LOANS : member_accounts
HUB -> APPS : applications
HUB -> SERVICING : servicing_cases

' Cards branch
CARDS_TXN -> CARDS : rolls up
REWARDS -> CARDS : enriches
CARDS_CAMP -> CARDS : influences

' Deposits branch
DEPOSITS_TXN -> DEPOSITS : rolls up
DEPOSITS_CAMP -> DEPOSITS : influences

' Loans branch
DAILY_DATA -> LOANS : rolls up
LOANS_CAMP -> LOANS : influences

' Applications branch
RISK_BUREAU -> APPS : enriches
BRANCH_ORIG -> APPS : operational feed

' Snapshots
ATOMIC_D -> CARDS : feeds
ATOMIC_D -> DEPOSITS : feeds
ATOMIC_D -> LOANS : feeds
ATOMIC_D -> SERVICING : feeds
PERIODIC_M -> APPS : feeds
PERIODIC_M -> HUB : feeds
AGENT_MONTHLY -> HUB : feeds
