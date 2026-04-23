# Phase 1 Implementation Plan: Case Family Router

This document converts the multi-case investigation blueprint into an exact Phase 1 implementation plan.

Phase 1 is intentionally limited. It should improve case routing without disturbing the current working transaction-fraud experience.

## 1. Phase 1 Goal

Add an early classification layer that decides:

- `case_family`
- `suspicion_direction`
- `investigation_basis`
- `transaction_relevance`

Then use those fields to control:

- the top summary/header shown to the investigator
- whether the transaction card is shown as primary, supporting, or secondary-only
- whether the response wording is transaction-led or not

This phase should not yet add new Mongo collections like `loan_accounts` or `document_verifications`.

## 2. Phase 1 Non-Goals

Do not do these in Phase 1:

- no new database schema for loans or collateral
- no document verification engine
- no loan exposure card with real backend data
- no historical reference overhaul
- no new seeding for complex loan/document cases yet
- no change to current report export pipeline beyond reading the new routing fields

Phase 1 is a router and UI-structure upgrade, not a full loan-fraud engine.

## 3. Expected Outcome After Phase 1

When the investigator enters a case narrative, the system should first infer the family and basis.

Examples:

- `Customer reported repeated UPI debits...`
  - `case_family = Transaction Fraud`
  - `suspicion_direction = Customer Victim`
  - `investigation_basis = Transaction-Led`
  - `transaction_relevance = primary`

- `Customer defaulted on home loan and submitted fake property papers`
  - `case_family = Loan / Mortgage Fraud`
  - `suspicion_direction = Customer-to-Bank`
  - `investigation_basis = Document-Led`
  - `transaction_relevance = not_applicable`

- `Customer denies card transaction but device/session trail looks normal`
  - `case_family = Dispute / First-Party Abuse`
  - `suspicion_direction = Customer-to-Bank`
  - `investigation_basis = Complaint-Led`
  - `transaction_relevance = supporting`

The UI should then show the transaction module appropriately rather than always behaving as a pure transaction fraud review.

## 4. Current Touchpoints To Change

## 4.1 Backend Routing and Analysis

Primary file:

- [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1860>)

Current relevant areas:

- integrated state and missing-field logic:
  - [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1407>)
  - [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1549>)
- transaction analysis:
  - [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1042>)
- SOP grounding:
  - [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1648>)
  - [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1701>)
- final integrated return:
  - [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:2178>)

## 4.2 API Schema

Primary file:

- [customer_fraud.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/api/customer_fraud.py:15>)

Current relevant response payload:

- [customer_fraud.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/api/customer_fraud.py:126>)
- [customer_fraud.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/api/customer_fraud.py:142>)

## 4.3 Frontend Rendering

Primary file:

- [index.html](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/ui/index.html:1>)

Current transaction-heavy sections:

- `Transaction Timeline` render around [index.html](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/ui/index.html:4251>)
- `Customer Baseline` render around [index.html](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/ui/index.html:4315>)
- `Suspicious Patterns` around [index.html](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/ui/index.html:4369>)
- `Flagged Transactions` around [index.html](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/ui/index.html:4391>)

## 5. New Fields To Introduce In Phase 1

Add these to the main integrated analysis payload:

- `case_family`
- `suspicion_direction`
- `investigation_basis`
- `transaction_relevance`
- `evidence_modules_used`
- `case_summary`

Recommended values:

- `case_family`
  - `Transaction Fraud`
  - `Loan / Mortgage Fraud`
  - `Document Fraud`
  - `KYC / Identity Fraud`
  - `Dispute / First-Party Abuse`
  - `Mule / Funnel Account`
  - `Mixed`
  - `Manual Review`
- `suspicion_direction`
  - `Customer Victim`
  - `Customer-to-Bank`
  - `Third-Party Abuse`
  - `Mixed`
  - `Manual Review`
- `investigation_basis`
  - `Transaction-Led`
  - `Document-Led`
  - `Loan-Led`
  - `Complaint-Led`
  - `Profile-Led`
  - `Mixed`
- `transaction_relevance`
  - `primary`
  - `supporting`
  - `not_applicable`

## 6. Phase 1 Backend Changes

## 6.1 Add a Family Classifier

File:

- [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1>)

Add helper functions:

- `_classify_case_family(case_description: str) -> dict[str, str]`
- `_transaction_relevance_for_family(case_family: str, basis: str) -> str`
- `_recommended_evidence_modules(case_family: str, transaction_relevance: str) -> list[str]`

Initial implementation should be deterministic and keyword-driven, not LLM-first.

Reason:

- stable for demo
- easy to debug
- no Gemini dependency for routing

First-pass keyword examples:

- `loan`, `mortgage`, `emi`, `collateral`, `property`, `sanction`, `repayment`, `default`
  - route to `Loan / Mortgage Fraud`
- `document`, `forged`, `fake papers`, `registry`, `ownership mismatch`, `valuation`, `salary slip`, `itr`, `gst`
  - route to `Document Fraud`
- `kyc`, `identity`, `pan mismatch`, `aadhaar`, `account opening`, `profile update`
  - route to `KYC / Identity Fraud`
- `chargeback`, `merchant dispute`, `customer denies`, `friendly fraud`, `false dispute`
  - route to `Dispute / First-Party Abuse`
- `mule`, `funnel`, `incoming credits`, `cash-out`, `pass-through`
  - route to `Mule / Funnel Account`
- payment terms like `upi`, `imps`, `atm`, `netbanking`, `beneficiary`, `unauthorized debit`
  - route to `Transaction Fraud`

If multiple families match:

- choose `Mixed`

If no strong match:

- choose `Manual Review`

## 6.2 Extend Integrated State

File:

- [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1407>)

Add to `_empty_integrated_state(...)`:

- `case_family`
- `suspicion_direction`
- `investigation_basis`
- `transaction_relevance`
- `evidence_modules_used`

Also normalize these fields in the state-loading helpers.

## 6.3 Classify Early In The Chat Flow

File:

- [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1860>)

Current flow asks for case description and customer/date inputs, then fetches transactions.

New Phase 1 step:

After `case_description` is captured:

1. classify family
2. store routing fields in state
3. continue asking for customer/date data as needed

Important:

- For Phase 1, customer identification can still remain mandatory for most cases.
- Do not redesign the full intake yet.
- Keep input collection stable.

## 6.4 Make Transaction Analysis Conditional

File:

- [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:2178>)

Current behavior:

- always fetch transactions
- always analyze transactions

New Phase 1 behavior:

- if `transaction_relevance == primary`
  - full current analysis
- if `transaction_relevance == supporting`
  - still fetch/analyze transactions, but mark card as supporting
- if `transaction_relevance == not_applicable`
  - do not fail the case just because transactions are empty
  - build a lightweight non-transaction placeholder analysis
  - set reasoning text like:
    - `This case is being reviewed primarily through non-transaction evidence based on the reported loan/document profile.`

This avoids the current bad outcome:

- `No transactions found` becoming the main answer for loan/document cases

## 6.5 Make SOP Grounding Family-Aware

File:

- [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1648>)

Current `_build_sop_grounding_query(...)` is transaction-window heavy.

Change it so it can build different prompts depending on `investigation_basis`.

Examples:

- `Transaction-Led`
  - current prompt remains mostly unchanged
- `Document-Led`
  - include case description, customer identity, document cues, and say transaction review is not primary
- `Loan-Led`
  - include loan/collateral/default narrative and say transaction review is secondary or unavailable
- `Complaint-Led`
  - include dispute narrative and any supporting transaction signal

Phase 1 will still use limited evidence, but the query should stop pretending every case is a transaction case.

## 6.6 Make Combined Analysis Aware Of Routing

File:

- [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1701>)

Extend `_combine_transaction_and_sop_analysis(...)` so it includes:

- `case_family`
- `suspicion_direction`
- `investigation_basis`
- `transaction_relevance`
- `evidence_modules_used`
- `case_summary`

Important rule:

- if the case family is non-transaction-led, do not default the combined family back to `Transaction-Led Review`
- let SOP and router preserve the selected family

## 7. Phase 1 API Changes

File:

- [customer_fraud.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/api/customer_fraud.py:126>)

Extend `CustomerFraudAnalysisPayload` with:

- `case_family: str`
- `suspicion_direction: str`
- `investigation_basis: str`
- `transaction_relevance: str`
- `evidence_modules_used: list[str]`
- `case_summary: str`

Also extend `CustomerFraudConversationState` with the same routing fields where useful.

Reason:

- frontend needs them for rendering
- conversation persistence stays stable

## 8. Phase 1 Frontend Changes

File:

- [index.html](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/ui/index.html:1>)

## 8.1 Add Universal Case Header

Show these at the top of the structured fraud card:

- `Case Family`
- `Suspicion Direction`
- `Investigation Basis`
- `Risk Level`

This should appear before the transaction sections.

## 8.2 Support Transaction Card Modes

Use `transaction_relevance` to control rendering:

- `primary`
  - current transaction sections stay visible as they are
- `supporting`
  - show a compact transaction review label like:
    - `Supporting Transaction Review`
  - keep timeline/patterns concise
- `not_applicable`
  - hide timeline/baseline/flagged sections as primary content
  - show a small note:
    - `Transaction review is not the primary evidence source for this case.`

Do not remove the structured card entirely. Only change internal sections.

## 8.3 Introduce Placeholder Non-Transaction Sections

Even before real loan/document collections exist, the UI should have safe placeholders when family requires them:

- `Loan / Mortgage Fraud`
  - `Primary Review Basis: Loan and collateral narrative`
- `Document Fraud`
  - `Primary Review Basis: Submitted document mismatch narrative`
- `KYC / Identity Fraud`
  - `Primary Review Basis: Identity and profile verification narrative`
- `Dispute / First-Party Abuse`
  - `Primary Review Basis: Complaint and authorization review`

These are Phase 1 placeholders only, but they stop the UI from looking broken.

## 9. Suggested Coding Order

Use this order to reduce regression risk:

1. add payload fields in backend service objects
2. add API schema fields
3. add deterministic case-family classifier
4. store routing fields in integrated state
5. make combined analysis include routing fields
6. update frontend to render universal header
7. update frontend transaction card visibility rules
8. test all current transaction flows again

## 10. Acceptance Criteria

Phase 1 is done only if all of the following are true:

1. Existing transaction-fraud demo still works as before.
2. A UPI fraud case still shows timeline, baseline, patterns, and flagged transactions normally.
3. A loan-fraud style narrative no longer shows `No transactions found` as the main conclusion.
4. The structured card clearly shows:
   - `Case Family`
   - `Suspicion Direction`
   - `Investigation Basis`
5. The UI can explain when transaction review is not primary.
6. SOP grounding prompt is no longer purely transaction-window-centric for non-transaction cases.

## 11. Phase 1 Demo Test Cases

Use these exact test styles:

### Case A: Transaction Fraud

`Customer reported repeated UPI debits without approval. CIF1001 from 2026-04-17 00:00 to 2026-04-18 23:59.`

Expected:

- `Transaction Fraud`
- `Customer Victim`
- `Transaction-Led`
- `transaction_relevance = primary`

### Case B: Loan Fraud

`Customer defaulted on a home loan and the mortgaged property papers appear fake. CIF1004. Review the case from the loan verification side.`

Expected:

- `Loan / Mortgage Fraud`
- `Customer-to-Bank`
- `Loan-Led` or `Document-Led`
- transaction section not primary

### Case C: Document Fraud

`Borrower submitted forged salary slips and bank statements during retail loan processing. CIF1006.`

Expected:

- `Document Fraud`
- `Customer-to-Bank`
- `Document-Led`

### Case D: Dispute Abuse

`Customer is denying a card transaction that appears self-authorized and has raised similar disputes before. CIF1008.`

Expected:

- `Dispute / First-Party Abuse`
- `Customer-to-Bank`
- `Complaint-Led`
- transaction review shown as supporting

## 12. Recommended Output Of Phase 1

At the end of Phase 1, the system should still feel like the same AXIS workspace, but smarter:

- same chat
- same case desk
- same report flow
- same docs flow
- same historical references flow

But now:

- it knows what kind of case it is reviewing
- it knows whether transaction evidence is central or not
- it stops forcing every case into a payment-fraud template

That is the correct base for Phase 2.
