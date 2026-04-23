# AXIS Multi-Case Investigation Blueprint

This document defines how the current AXIS fraud workspace should evolve from a transaction-fraud reviewer into a multi-case investigation workspace that can also handle loan fraud, document fraud, KYC/identity abuse, dispute abuse, mule-account behavior, and mixed cases.

It is intended to be the implementation blueprint before code changes begin.

## 1. Why This Change Is Needed

The current integrated flow is strong for customer-victim transaction cases:

- unauthorized UPI, IMPS, ATM, POS, and netbanking debits
- new-beneficiary fraud
- rapid debit velocity
- suspicious beneficiary patterns
- customer baseline deviation

Current routing is still fundamentally transaction-led:

- SOP grounding is built from transaction-window analysis in [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1648>)
- combined analysis defaults back to `Transaction-Led Review` when no stronger family is identified in [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1701>)
- the final response path always fetches transactions before constructing the main fraud card in [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:2178>)

That works for payment fraud, but it is not enough for cases like:

- fake mortgage property submitted for a loan
- forged income, ITR, GST, or salary proofs
- fake KYC and identity onboarding
- customer-led false dispute or friendly fraud
- mule-account style inward-outward funneling
- shell vendor or diversion style loan abuse

The system therefore needs to decide first:

1. What kind of case is this?
2. Who is the likely suspicious side?
3. What is the primary evidence basis?
4. Which cards should the UI show for this case?

## 2. SOP-Grounded Direction

The local AXIS blueprint already supports a broader fraud taxonomy beyond payment fraud.

Relevant signals observed from the local SOP documents:

- The blueprint explicitly spans fraud taxonomy, reporting workflow, and AI-assisted case reporting.
- It includes broad case buckets such as payment fraud, KYC and identity fraud, loan and credit fraud, corporate fraud, and vendor/third-party fraud.
- It references specific loan/document categories such as forged income proofs, fake or duplicated collateral, loan end-use diversion, shell-vendor routing, and identity-based loan application fraud.
- It also includes investigation stages that are not purely transaction-led: onboarding/KYC review, collateral review, document verification, callback verification, field checks, and staff accountability review.
- The stricter SOP draft also points toward dual-track handling: external/borrower fraud and staff accountability.

Practical implication:

- The next system should not assume every case starts with transactions.
- It should route cases into the correct investigation family first, then use SOP grounding against that family.

## 3. Core Design Principle

The workspace should become a dynamic investigation orchestrator.

Every case should first be classified into:

- `case_family`
- `suspicion_direction`
- `investigation_basis`

Then the UI should render:

- one universal case header
- only the evidence cards that fit that case

This avoids forcing a transaction card on every investigation.

## 4. Universal Case Header

The top section should always show:

- `Case Family`
- `Suspicion Direction`
- `Investigation Basis`
- `Risk Level`
- `Case Status`
- `Customer / Entity`
- `Short Case Summary`

Recommended field values:

- `Case Family`: `Transaction Fraud`, `Loan / Mortgage Fraud`, `Document Fraud`, `KYC / Identity Fraud`, `Dispute / First-Party Abuse`, `Mule / Funnel Account`, `Corporate / Vendor Fraud`, `Mixed`, `Manual Review`
- `Suspicion Direction`: `Customer Victim`, `Customer-to-Bank`, `Third-Party Abuse`, `Staff / Internal`, `Mixed`, `Manual Review`
- `Investigation Basis`: `Transaction-Led`, `Document-Led`, `Loan-Led`, `Complaint-Led`, `Profile-Led`, `Mixed`

## 5. Case Family Matrix

### 5.1 Transaction Fraud

- Typical examples:
  - unauthorized UPI debit
  - IMPS fraud
  - ATM cash-out
  - card misuse
  - beneficiary fraud
  - account takeover
- Suspicion direction:
  - usually `Customer Victim`
- Investigation basis:
  - `Transaction-Led`
- Primary cards:
  - `Transaction Review`
  - `Transaction Timeline`
  - `Customer Baseline`
  - `Flagged Transactions`
  - `SOP Grounding`

### 5.2 Loan / Mortgage Fraud

- Typical examples:
  - fake collateral
  - forged property papers
  - forged income or bank statements
  - shell-vendor routing
  - end-use diversion
- Suspicion direction:
  - usually `Customer-to-Bank`
  - sometimes `Mixed`
- Investigation basis:
  - `Loan-Led` or `Document-Led`
- Primary cards:
  - `Loan Exposure`
  - `Collateral Review`
  - `Document Verification`
  - `Repayment / Default Snapshot`
  - `SOP Grounding`
- Transaction card behavior:
  - hidden if no relevant transactional evidence exists
  - compact only if repayment or fund diversion trail matters

### 5.3 Document Fraud

- Typical examples:
  - forged document submission
  - manipulated proof-of-address
  - altered ownership or registration papers
  - forged trade or FX support documents
- Suspicion direction:
  - usually `Customer-to-Bank`
  - sometimes `Third-Party Abuse`
- Investigation basis:
  - `Document-Led`
- Primary cards:
  - `Document Verification`
  - `Mismatch Findings`
  - `Linked Exposure`
  - `SOP Grounding`

### 5.4 KYC / Identity Fraud

- Typical examples:
  - fake KYC
  - synthetic identity
  - mule-account onboarding
  - profile change abuse
- Suspicion direction:
  - `Customer-to-Bank`, `Third-Party Abuse`, or `Mixed`
- Investigation basis:
  - `Profile-Led`
- Primary cards:
  - `Identity / KYC Review`
  - `Profile Events`
  - `Linked Accounts`
  - `SOP Grounding`

### 5.5 Dispute / First-Party Abuse

- Typical examples:
  - customer denies likely self-authorized transaction
  - false merchant dispute
  - chargeback misuse
  - repeated friendly-fraud complaints
- Suspicion direction:
  - usually `Customer-to-Bank`
- Investigation basis:
  - `Complaint-Led` with supporting transaction evidence
- Primary cards:
  - `Dispute Pattern`
  - `Complaint History`
  - `Transaction Review`
  - `Merchant / Authorization Evidence`
  - `SOP Grounding`

### 5.6 Mule / Funnel Account

- Typical examples:
  - repeated incoming credits followed by fast outward movement
  - linked-beneficiary payout pattern
  - rapid cash-out after inward receipts
- Suspicion direction:
  - usually `Customer-to-Bank` or `Mixed`
- Investigation basis:
  - `Transaction-Led`
- Primary cards:
  - `Flow Review`
  - `Transaction Timeline`
  - `Linked Accounts / Beneficiaries`
  - `SOP Grounding`

### 5.7 Corporate / Vendor Fraud

- Typical examples:
  - false invoice
  - BEC
  - outsourced partner misconduct
  - DSA-assisted sourcing misconduct
- Suspicion direction:
  - `Third-Party Abuse`, `Customer-to-Bank`, or `Mixed`
- Investigation basis:
  - `Document-Led`, `Transaction-Led`, or `Mixed`
- Primary cards:
  - `Document Review`
  - `Approval Trail`
  - `Transaction Review`
  - `Counterparty Review`
  - `SOP Grounding`

### 5.8 Mixed / Manual Review

- Use when:
  - more than one family has strong evidence
  - or evidence is incomplete
- Suspicion direction:
  - `Mixed` or `Manual Review`
- Investigation basis:
  - `Mixed`
- Primary cards:
  - whichever two or three modules are most relevant
  - never show all cards by default

## 6. Card Routing Rules

## 6.1 Transaction Card Must Be Primary When

- reviewed transactions exist in the requested window
- suspicious debit/credit movement is central to the case
- the case family is `Transaction Fraud` or `Mule / Funnel Account`
- false dispute review requires transaction evidence

## 6.2 Transaction Card Must Be Compact When

- transaction evidence exists but is not the main source of suspicion
- loan or document fraud also needs a limited transaction trail
- mixed cases need both transaction and non-transaction evidence

Compact transaction card should show:

- transaction count
- transaction relevance note
- only top flagged items
- note that transaction review is supporting evidence, not primary evidence

## 6.3 Transaction Card Must Not Be Primary When

- the case is document-led
- the case is loan-led
- there is no meaningful transaction trail
- fake collateral or forged docs are the central issue
- KYC/identity mismatch is the primary trigger

In those cases, show:

- `Transaction Review Not Primary For This Case`
- one short sentence explaining why

## 6.4 Card Priority by Basis

- `Transaction-Led`
  - Transaction Review
  - Timeline
  - Baseline
  - SOP
- `Loan-Led`
  - Loan Exposure
  - Collateral Review
  - Document Verification
  - Repayment Snapshot
  - SOP
- `Document-Led`
  - Document Verification
  - Mismatch Findings
  - Linked Exposure
  - SOP
- `Complaint-Led`
  - Complaint Summary
  - Dispute Pattern
  - Transaction Review
  - SOP
- `Profile-Led`
  - Identity/KYC Review
  - Profile Events
  - Linked Accounts
  - SOP
- `Mixed`
  - top two primary evidence modules
  - one compact supporting module
  - SOP

## 7. Current vs Future Chatbot Flow

### 7.1 Current Effective Flow

- collect customer and date range
- fetch transactions
- analyze transaction behavior
- ground via SOP
- show transaction-heavy card

### 7.2 Future Desired Flow

1. Collect case narrative
2. Identify entity and required identifiers
3. Run early `case_family` classification
4. Decide `investigation_basis`
5. Ask only the next required inputs for that family
6. Fetch relevant data modules
7. Render dynamic evidence cards
8. Run SOP grounding with family-aware context
9. Generate report / docs / historical references

## 8. Additional Data Needed

The current `customers` and `transactions` collections are not enough for the full target system.

Recommended additions:

### 8.1 `case_events`

Purpose:

- case intake
- complaint/dispute context
- investigator findings
- outcome tracking

Suggested fields:

- `case_id`
- `cif_id`
- `account_id`
- `loan_id`
- `case_family`
- `suspicion_direction`
- `investigation_basis`
- `reported_at`
- `complaint_type`
- `dispute_reason`
- `linked_txn_ids`
- `reported_by`
- `outcome`
- `notes`

### 8.2 `loan_accounts`

Purpose:

- loan exposure and repayment review

Suggested fields:

- `loan_id`
- `cif_id`
- `loan_type`
- `sanction_amount`
- `outstanding_amount`
- `emi_amount`
- `overdue_amount`
- `dpd`
- `status`
- `disbursement_date`

### 8.3 `collateral_records`

Purpose:

- mortgage/collateral evidence

Suggested fields:

- `collateral_id`
- `loan_id`
- `cif_id`
- `property_type`
- `declared_owner`
- `document_reference`
- `valuation_amount`
- `verification_status`
- `mismatch_reason`

### 8.4 `document_verifications`

Purpose:

- document-level investigation results

Suggested fields:

- `verification_id`
- `cif_id`
- `loan_id`
- `document_type`
- `submitted_at`
- `verification_status`
- `mismatch_type`
- `finding_summary`
- `verified_by`

### 8.5 `profile_events`

Purpose:

- KYC and account-profile change analysis

Suggested fields:

- `event_id`
- `cif_id`
- `event_type`
- `channel`
- `timestamp`
- `old_value`
- `new_value`
- `verified`

## 9. Proposed New Analysis Fields

The main integrated analysis payload should eventually include:

- `case_family`
- `suspicion_direction`
- `investigation_basis`
- `evidence_modules_used`
- `transaction_relevance`
- `case_summary`
- `risk_level`
- `fraud_category`
- `fraud_classification`
- `evidence_summary`
- `recommended_actions`

Recommended `transaction_relevance` values:

- `primary`
- `supporting`
- `not_applicable`

## 10. UI Changes Needed

### 10.1 Universal Header

Add a common summary strip above the evidence cards:

- Case Family
- Suspicion Direction
- Investigation Basis
- Risk Level
- Case Status

### 10.2 Dynamic Evidence Cards

Render only the cards selected by the router.

New cards likely required:

- `Loan Exposure`
- `Collateral Review`
- `Document Verification`
- `Mismatch Findings`
- `Repayment Snapshot`
- `Complaint / Dispute Pattern`
- `Identity / KYC Review`
- `Profile Events`
- `Linked Accounts`

### 10.3 Transaction Review Behavior

Do not remove the transaction card entirely from the system.
Instead support three modes:

- `primary`
- `supporting`
- `hidden`

## 11. Historical References Strategy

Historical references should also become family-aware.

Current references are strongest for transaction-victim cases.

Next dataset should include:

- fake collateral / forged mortgage case
- forged salary/income proof
- KYC / identity onboarding fraud
- repeat false dispute case
- mule-account funnel case
- shell-vendor or end-use diversion case

Historical references should be filtered by:

- `case_family`
- `suspicion_direction`
- `investigation_basis`

## 12. Recommended Phased Implementation

## Phase 0: Design Approval

Goal:

- lock taxonomy
- lock routing rules
- lock new data model

Output:

- this blueprint approved

## Phase 1: Family Classifier and Card Router

Goal:

- classify cases before transaction analysis
- decide whether transaction card is primary, supporting, or hidden

Code areas likely affected:

- [customer_fraud_chat_service.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/services/customer_fraud_chat_service.py:1648>)
- [app/api/customer_fraud.py](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/api/customer_fraud.py:1>)
- [app/ui/index.html](</C:/Users/Yashd/OneDrive/Desktop/AXIS_VERCEL/app/ui/index.html:1>)

## Phase 2: Data Model Expansion and Seed Data

Goal:

- add `case_events`
- add `loan_accounts`
- add `collateral_records`
- add `document_verifications`
- add `profile_events`
- seed realistic demo cases for each family

## Phase 3: Family-Aware SOP Grounding

Goal:

- build SOP grounding queries from the correct evidence family, not only transactions
- support document-led and loan-led prompts

## Phase 4: Historical References and Report Expansion

Goal:

- return family-matched references
- generate richer reports for non-transaction cases

## Phase 5: QA and Demo Hardening

Goal:

- validate all family flows
- prevent regressions in current transaction fraud workflow

## 13. Testing Matrix

Minimum mandatory test set:

1. `Customer Victim / Transaction Fraud`
   - rapid UPI debit with new beneficiary
2. `Customer-to-Bank / Loan Fraud`
   - fake collateral, no meaningful transaction trail
3. `Customer-to-Bank / Document Fraud`
   - forged salary proof or KYC mismatch
4. `Customer-to-Bank / Dispute Abuse`
   - repeated false dispute pattern
5. `Mule / Funnel Account`
   - inward receipts and fast outward movement
6. `Mixed Case`
   - document issue plus suspicious transaction trail
7. `Manual Review`
   - insufficient evidence

## 14. Recommended Immediate Next Step

Do not jump straight to full data-model coding.

The safest first coding step is:

1. implement a lightweight `case_family` and `investigation_basis` classifier from the case narrative
2. add `transaction_relevance`
3. route the UI to show:
   - transaction card as `primary`
   - transaction card as `supporting`
   - or no primary transaction card

This gives immediate benefit without breaking the working transaction-fraud flow.

Detailed Phase 1 breakdown:

- `docs/phase-1-case-family-router-plan.md`

## 15. Final Recommendation

If the goal is a strong bank-manager and supervisor demo, the system should be positioned as:

- not only a payment-fraud chatbot
- but an AI-assisted fraud investigation workspace

That workspace should:

- classify the case family first
- understand who is likely suspicious
- choose the correct evidence basis
- show only the relevant investigation cards
- then ground everything in the AXIS SOP

That is the right foundation for handling both:

- customer-victim fraud
- customer-to-bank fraud

without forcing every case into the same transaction template.
