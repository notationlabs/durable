process.on("message", () => {
  process.send({
    status: "outcome",
    outcome: {
      status: "diverged",
      code: "NONDETERMINISTIC_REPLAY",
      stepKey: "charge-v3",
    },
  });
});

process.send({ status: "ready" });
