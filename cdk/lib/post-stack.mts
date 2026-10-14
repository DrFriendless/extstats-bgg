import {
    CloudWatchLogsClient,
    DescribeLogGroupsCommand,
    PutRetentionPolicyCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import {LambdaClient, UpdateEventSourceMappingCommand} from "@aws-sdk/client-lambda";
import {CloudFormationClient, ListExportsCommand} from "@aws-sdk/client-cloudformation";
import {
    REGION,
    PROFILE
} from "./metadata.mts";

console.log("POST STACK");
const CREDENTIALS = { region: REGION, profile: PROFILE };
const lClient = new LambdaClient(CREDENTIALS);

// set the retention period on the log groups
console.log("Setting retention period on log groups");
const cwClient = new CloudWatchLogsClient(CREDENTIALS);
for (const prefix of ["RustDownloaderStack-"]) {
    const cmd = new DescribeLogGroupsCommand({ logGroupNamePrefix: `/aws/lambda/${prefix}`,  });
    const gs = await cwClient.send(cmd);
    if (gs && gs.logGroups) {
        for (const g of gs.logGroups) {
            const pcmd = new PutRetentionPolicyCommand({ logGroupName: g.logGroupName, retentionInDays: 1 });
            const r = await cwClient.send(pcmd);
            console.log(r.$metadata.httpStatusCode, g.logGroupName);
        }
    }
}

const client = new CloudFormationClient(CREDENTIALS);
const command = new ListExportsCommand({ });
const response = await client.send(command);
for (const e of response.Exports || []) {
    if (e.Name === "downloader-AddTimestampMappingUUID") {
        // turn on the add timestamp processing from the queue 1
        console.log("Turning on add timestamp queue");
        const lCmd = new UpdateEventSourceMappingCommand({
            Enabled: true,
            UUID: e.Value
        });
        const r3 = await lClient.send(lCmd);
        console.log(r3.$metadata.httpStatusCode);
    } else if (e.Name === "downloader-DownloadMappingUUID") {
        // turn on the dowmload processing from the queue 1
        console.log("Turning on download queue");
        const lCmd = new UpdateEventSourceMappingCommand({
            Enabled: true,
            UUID: e.Value
        });
        const r3 = await lClient.send(lCmd);
        console.log(r3.$metadata.httpStatusCode);
    } else {
        console.log(e.Name);
    }
}

console.log("POST STACK DONE");