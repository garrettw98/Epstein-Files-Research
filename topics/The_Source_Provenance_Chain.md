# The Source Provenance Chain

> **Status**: **METHODOLOGY STANDARD**

## Summary

This file defines how the repository should track source provenance from raw acquisition to claim-level inference so readers can audit what is known, what is inferred, and what remains unresolved.

## Provenance Layers

1. **Acquisition**: where/when/how the artifact was fetched.
2. **Normalization**: transformations from raw source into derived rows.
3. **Citation**: claim-evidence links to specific documents/spans.
4. **Inference**: analytical interpretation with explicit confidence and status.

## Minimum Provenance Checklist for High-Risk Claims

- Stable source URL and capture timestamp.
- Document ID with checksum or equivalent integrity marker.
- Evidence locator and/or span reference where possible.
- Clear claim status (`verified_primary`, `verified_secondary`, `pending_review`, etc.).
- Explicit note when evidence is secondary-only.

## Name-Context Taxonomy

Use the `name_context_class` field in claim records:

- `direct_contact`: direct meeting/travel/communication/transaction context.
- `administrative_mention`: procedural or institutional reference without direct-contact implication.
- `media_reference`: claim currently grounded in secondary reporting coverage.
- `unverified_allegation`: unresolved allegation lacking sufficient corroboration.
- `unknown`: not yet classified.

## Redaction Taxonomy

Recommended categories for release-process analysis:

- `victim_privacy`
- `ongoing_investigation`
- `national_security`
- `context_gap`
- `unknown`

## Sources

- [Primary Sources Index](../evidence/Primary_Sources_Index.md)
- [Claim Linking Format](../schema/CLAIM_LINKING_FORMAT.md)
- [Repository Organization](../docs/REPO_ORGANIZATION.md)
- [Data Pipeline Runbook](../docs/DATA_PIPELINE.md)

## See Also

- [Redaction and Context Scandal](The_Redaction_and_Context_Scandal.md)
- [The DOJ 300+ Name Letter](The_DOJ_300_Name_Letter.md)
- [The FBI Scrubbing Allegation Evidence Chain](The_FBI_Scrubbing_Allegation_Evidence_Chain.md)
