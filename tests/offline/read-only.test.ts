import { expect, test } from "vitest";
import { uniqueHash } from "../support/artifacts.ts";
import { IN_FLIGHT_UPLOAD_LENGTH, KeepAliveConnection } from "../support/keep-alive-connection.ts";
import { startServer } from "../support/nx-cache-aws.ts";
import { VALID_ENV } from "../support/server-env.ts";

test("a read-only upload is received in full, then refused with 403", async () => {
    const server = await startServer(VALID_ENV);
    const connection = new KeepAliveConnection(server.port);

    const upload = await connection.putStreaming(
        uniqueHash("read-only-upload"),
        "read-only",
        IN_FLIGHT_UPLOAD_LENGTH,
    );

    expect(upload.status).toBe(403);
});

// meddevo fork: every response carries Connection: close (188768c, ALB keep-alive 502s),
// so the connection is never reused.
// oxlint-disable-next-line vitest/no-disabled-tests
test.skip("the connection stays usable after a read-only upload is refused", async () => {
    const server = await startServer(VALID_ENV);
    const connection = new KeepAliveConnection(server.port);
    await connection.putStreaming(
        uniqueHash("read-only-upload"),
        "read-only",
        IN_FLIGHT_UPLOAD_LENGTH,
    );

    const health = await connection.get("/health", "none");

    expect(health).toEqual({ status: 200, reusedConnection: true });
});
