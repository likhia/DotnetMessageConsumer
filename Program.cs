using System;
using System.Collections.Generic;
using Newtonsoft.Json;

using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Pulsar.Client.Api;
using System.Text;
using Microsoft.Extensions.Logging;
using Pulsar.Client.Common;

const string TABLE_NAME = "<TABLE NAME>"; 

const string SERVICE_URL = "pulsar://<IP Address / Name of Host 1>:6650,<IP Address / Name of Host 2>:6650,<IP Address / Name of Host 3>:6650";
const string SCHEMA_NAME = "<Tenant>/<Namespace>/<Schema>." + TABLE_NAME;
const string TOPIC_NAME = "persistent://" + SCHEMA_NAME;

//It depends on where you want to place your exported data.
const string OUTPUT_FILE_NAME = "/root/code/dotnet/output/" + TABLE_NAME + "_transaction.csv";

const string SUBSCRIPTION_NAME = "my-subscription";

// Create client object
var client =  await new PulsarClientBuilder()
                                .ServiceUrl(SERVICE_URL)
                                .BuildAsync();


// Create consumer on a topic with a subscription
var consumer = await client.NewConsumer()
	.Topic(TOPIC_NAME)
	.SubscriptionName(SUBSCRIPTION_NAME)
	.SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
	.SubscribeAsync();

var receivedMsg = false;
var cnt = 0; 
while (!receivedMsg)
{
    var message = await consumer.ReceiveAsync();

    var msgKeyDecoded = Convert.FromBase64String(message.Key);

    string key = Encoding.UTF8.GetString(msgKeyDecoded);
    string value = Encoding.UTF8.GetString(message.Data);

    ProcessKey(key);
    Dictionary<string,string>  contentDict = ProcessValue(value);

    using StreamWriter wfile = new(OUTPUT_FILE_NAME, append: true);
    string content = "";
    

    //Writing the column name as 1st row in csv.
    try {
        using StreamReader rfile = new(OUTPUT_FILE_NAME);
        string rline = rfile.ReadLine();
        if(rline is null) {
            foreach (string dictKey in contentDict.Keys) {
                content = content  + dictKey + ",";
            }

            content = content.Substring(0, content.Length-1);
            await wfile.WriteLineAsync(content);
        }
        rfile.Close();
    } catch(Exception e) {
        Console.WriteLine(e.Message);
    }

    string Action = contentDict["Action"];

    if(Action.Equals("read")) {
        await consumer.AcknowledgeAsync(message.MessageId);
        continue;
    }

    content = "";
    
    //If the value contains commas(,) then replace it with %2C so that it will not cause misalignment of data in csv.   
    foreach (string dictKey in contentDict.Keys) {
        string val = contentDict[dictKey];

        if(val != null) {
            val = contentDict[dictKey].Replace("," , "%2C");
        }

        content = content  + val + ",";
    }



    content = content.Substring(0, content.Length-1);
    await wfile.WriteLineAsync(content);

    // Acknowledge the message to remove it from the message backlog
    await consumer.AcknowledgeAsync(message.MessageId);

    //receivedMsg = true;
}

//Close the consumer
await consumer.DisposeAsync();

// Close the client
await client.CloseAsync();

static Dictionary<string,string> ProcessKey(string keyStr) {

    Dictionary<string,string> dict = JsonConvert.DeserializeObject<Dictionary<string, string>>(keyStr);

    if(dict is null) {
        return null;
    }

    //foreach (string key in dict.Keys) {
    //    Console.WriteLine(key + " : " + dict[key]);

    //}
    return dict;

}

static Dictionary<string,string> ProcessValue(string value) {


            var index = value.IndexOf(",\"op\"");

            var action = "read";
            string opValue = value.Substring(index+7, 1);
            
            //Console.WriteLine(opValue);
            
            if(opValue.Equals("r")) {
                action = "read";
            } else if(opValue.Equals("c")) {
                action = "create";
            } else if(opValue.Equals("u")) {
                action = "update";
            } else if(opValue.Equals("d")) {
                action = "delete";
            }

            Console.WriteLine(action);


            value = value.Substring(0, index) + "}";
            

            Dictionary<string,Dictionary<string, string>> dict = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, string>>>(value);

            
            //var action = "read";
            var field = "";
            if (dict["before"] is null && dict["after"] is not null) {
                //action = "create";
                field = "after";
            } else if (dict["before"] is not null && dict["after"] is null) {
                //action = "delete";
                field = "before";
            } else if (dict["before"] is not null && dict["after"] is not null) {
                //action = "update";
                field = "after";
            }

            Dictionary<string,string> contentDict = dict[field];


            contentDict.Add("Action", action);


        return contentDict;

}


