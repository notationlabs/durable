process.on("message", (message) => {
  process.send({
    status: "outcome",
    outcome: {
      status: "diverged",
      code: "NONDETERMINISTIC_REPLAY",
      stepKey: "charge-v3",
      workflowVersion: message.workflowVersion,
    },
  });
});

process.send({ status: "ready" });
