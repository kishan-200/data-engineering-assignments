# Submission checklist

- [ ] Run `pip install -r requirements.txt`
- [ ] Run `python run_all.py`
- [ ] Inspect every file in `results/`
- [ ] Read the limitations and make sure the submission describes them honestly
- [ ] Add screenshots of successful runs if required by your instructor
- [ ] Add your name/course details to README if needed
- [ ] Push to GitHub and verify files online

## Known scope limitations
- P4 uses deterministic fallback routing, not an external LLM benchmark.
- P5 uses TF-IDF retrieval, not a clinical embedding model.
- P6 uses a lightweight hybrid retrieval/reranking baseline, not a trained cross-encoder.
- P7/P8 are controlled tool/agent demonstrations, not autonomous production agents.
- P9 prepares data/configuration but does not run a full QLoRA fine-tuning job.
- P10 uses a small proxy evaluation, not the full RAGAS suite.
