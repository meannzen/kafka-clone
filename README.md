[![progress-banner](https://backend.codecrafters.io/progress/kafka/26b21073-0b7b-437e-9ddb-16ee3947a283)](https://app.codecrafters.io/users/goose-977?r=2qF)

This is a starting point for Rust solutions to the
["Build Your Own Kafka" Challenge](https://codecrafters.io/challenges/kafka).

In this challenge, you'll build a toy Kafka clone that's capable of accepting
and responding to ApiVersions & Fetch API requests. You'll also learn about
encoding and decoding messages using the Kafka wire protocol. You'll also learn
about handling the network protocol, event loops, TCP sockets and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Kafka implementation is in `src/main.rs`. Study and
uncomment the relevant code, and then run the command below to execute the tests
on our servers:

```sh
codecrafters submit
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `cargo (1.96)` installed locally
1. Run `./your_program.sh` to run your Kafka broker, which is implemented in
   `src/main.rs`. This command compiles your Rust project, so it might be slow
   the first time you run it. Subsequent runs will be fast.
1. Run `codecrafters submit` to submit your solution to CodeCrafters. Test
   output will be streamed to your terminal.
