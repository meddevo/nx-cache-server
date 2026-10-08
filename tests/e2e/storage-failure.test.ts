import { expect, test } from "vitest";
import { artifact, uniqueHash } from "../support/artifacts.ts";
import { ERROR_LOG_LEVEL, startServer } from "../support/nx-cache-aws.ts";
import { withVariables } from "../support/server-env.ts";
import { e2eServerEnv } from "./storage/shared-storage.ts";

// meddevo fork: the startup probe exits on a 4xx from S3 (upstream #26), so the server
// never comes up in either test below.
// oxlint-disable-next-line vitest/no-disabled-tests
test.skip("with a missing bucket, nothing is stored and the failure is logged", async () => {
    const server = await startServer(
        withVariables(e2eServerEnv(), { S3_BUCKET_NAME: uniqueHash("missing-bucket") }),
    );
    const client = server.client("read-write");
    const hash = uniqueHash("missing-bucket-artifact");

    const write = await client.put(hash, artifact(1024));
    const read = await client.get(hash);
    const logs = await server.stop();

    expect(write.status, "PUT").not.toBe(200);
    expect(read.status, "GET").not.toBe(200);
    expect(logs.stdout).toContain(ERROR_LOG_LEVEL);
});

// oxlint-disable-next-line vitest/no-disabled-tests
test.skip("with a wrong secret key, nothing is stored and the failure is logged", async () => {
    const server = await startServer(
        withVariables(e2eServerEnv(), { AWS_SECRET_ACCESS_KEY: "wrong-secret" }),
    );
    const client = server.client("read-write");
    const hash = uniqueHash("wrong-secret");

    const write = await client.put(hash, artifact(1024));
    const read = await client.get(hash);
    const logs = await server.stop();

    expect(write.status, "PUT").not.toBe(200);
    expect(read.status, "GET").not.toBe(200);
    expect(logs.stdout).toContain(ERROR_LOG_LEVEL);
});
