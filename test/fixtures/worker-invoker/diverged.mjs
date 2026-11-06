process.on("message", () => {
  process.send({
    status: "outcome",
    outcome: {
      status: "diverged",
      code: "NONDETERMINISTIC_REPLAY",
      stepKey: "charge-v3",
      message: 'Step "charge-v3" would run, but replay did not reach recorded step "charge"',
    },
  });
});

process.send({ status: "ready" });
