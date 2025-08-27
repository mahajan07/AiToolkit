
@startuml
skinparam packageStyle rectangle
skinparam linetype ortho
hide circle

' Core Semantic Hub
package "Semantic Hub" {
  class "MEMBER AGENT\n(Semantic Model)" as MEMBER_AGENT <<Hub>>
}

' Portfolio Masters
package "Portfolio Masters" {
  class "CARDS PORTFOLIO MASTER" as CARDS_PM
  class "DEPOSITS PORTFOLIO MASTER" as DEPOSITS_PM
  class "CONSUMER LOANS PORTFOLIO MASTER" as LOANS_PM
  class "APPLICATIONS & ORIGINATION MASTER" as APPS_PM
  class "SERVICING & COLLECTIONS MASTER" as SERVICING_PM
}

' Domain Inputs
package "Domain Inputs" {
  class "CARDS TRANSACTION MODEL" as CARDS_TXN
  class "DEPOSITS TRANSACTION MODEL" as DEPOSITS_TXN
  class "DAILY DATA MODEL" as DAILY_DATA
  class "REWARDS LEDGER" as REWARDS
  class "CAMPAIGNS (Cards)" as CARDS_CAMP
  class "CAMPAIGNS (Deposits)" as DEPOSITS_CAMP
  class "CAMPAIGNS (Loans)" as LOANS_CAMP
}

' Risk & Origination
package "Risk & Origination" {
  class "RISK & BUREAU SNAPSHOT" as RISK_BUREAU
  class "BRANCH LEVEL ORIGINATION" as BRANCH_ORIG
}

' Snapshots
package "Snapshots" {
  class "ATOMIC - D SNAPS" as ATOMIC_D
  class "PERIODIC - M SNAPS" as PERIODIC_M
  class "AGENTS / SEMANTIC - MONTHLY SNAPS" as AGENT_MONTHLY
}

' Hub connections to portfolio masters
MEMBER_AGENT -- CARDS_PM
MEMBER_AGENT -- DEPOSITS_PM
MEMBER_AGENT -- LOANS_PM
MEMBER_AGENT -- APPS_PM
MEMBER_AGENT -- SERVICING_PM

' Cards internals
CARDS_TXN --> CARDS_PM : rolls up
REWARDS --> CARDS_PM : enriches
CARDS_CAMP --> CARDS_PM : influences

' Deposits internals
DEPOSITS_TXN --> DEPOSITS_PM : rolls up
DEPOSITS_CAMP --> DEPOSITS_PM : influences

' Loans internals
DAILY_DATA --> LOANS_PM : rolls up
LOANS_CAMP --> LOANS_PM : influences

' Applications internals
RISK_BUREAU --> APPS_PM : enriches
BRANCH_ORIG --> APPS_PM : operational feed

' Servicing (direct)
' Already linked to MEMBER_AGENT

' Snapshots feeding models
ATOMIC_D --> CARDS_PM
ATOMIC_D --> DEPOSITS_PM
ATOMIC_D --> LOANS_PM
ATOMIC_D --> SERVICING_PM
PERIODIC_M --> APPS_PM
PERIODIC_M --> MEMBER_AGENT
AGENT_MONTHLY --> MEMBER_AGENT

@enduml
