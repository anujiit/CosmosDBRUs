using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Configuration;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkUpdate;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Diagnostics;

public class Program
{
    // ADD THIS PART TO YOUR CODE
    private const string EndpointUrl = "<endpointurl>";
    private const string PrimaryKey = "<AuthKey>";
    private DocumentClient client;

    static void Main(string[] args)
    {
        // ADD THIS PART TO YOUR CODE
        try
        {
            Program p = new Program();
            p.UsingAsyncTasks().Wait();
        }
        catch (DocumentClientException de)
        {
            Exception baseException = de.GetBaseException();
            Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
        }
        catch (Exception e)
        {
            Exception baseException = e.GetBaseException();
            Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
        }
        finally
        {
            Console.WriteLine("End of demo, press any key to exit.");
            Console.ReadKey();
        }
    }

    private async Task UsingAsyncTasks()
    {
        var option = new FeedOptions { EnableCrossPartitionQuery = true };
        this.client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
        //Fetch the Document to be updated
        var docs = this.client.CreateDocumentQuery<Document>(UriFactory.CreateDocumentCollectionUri("database", "collection"), option).ToList();

        foreach (var doc in docs)
        {
            //Update some properties on the found resource
            doc.SetPropertyValue("isDeleted", "false");
            //Now persist these changes to the database by replacing the original resource
            ResourceResponse<Document> response = await client.ReplaceDocumentAsync(doc);
            Console.WriteLine("Request Charge: " + response.RequestCharge);
        }
    }

    private async Task UsingBulkUpdate()
    {
        var option = new FeedOptions { EnableCrossPartitionQuery = true };
        this.client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
        DocumentCollection dataCollection = client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri("database"), option)
                .Where(c => c.Id == "collection").AsEnumerable().FirstOrDefault();
     
        long numberOfDocumentsToGenerate = 3;
        int numberOfBatches = 1;
        long numberOfDocumentsPerBatch = (long)Math.Floor(((double)numberOfDocumentsToGenerate) / numberOfBatches);

        // Set retry options high for initialization (default values).
        client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
        client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;
        var docs = client.CreateDocumentQuery(dataCollection.SelfLink);
        var listIds = new List<string>();
        foreach (var document in docs)
        {
            listIds.Add(document.GetPropertyValue<string>("id"));
        }
        IBulkExecutor bulkExecutor = new BulkExecutor(client, dataCollection);
        await bulkExecutor.InitializeAsync();

        // Set retries to 0 to pass control to bulk executor.
        client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
        client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

        double totalRequestUnitsConsumed = 0;
        double totalTimeTakenSec = 0;

        var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;

        BulkUpdateResponse bulkUpdateResponse = null;
        long totalNumberOfDocumentsUpdated = 0;
        totalRequestUnitsConsumed = 0;
        totalTimeTakenSec = 0;

        tokenSource = new CancellationTokenSource();
        token = tokenSource.Token;

        // Generate update operations.
        List<UpdateOperation> updateOperations = new List<UpdateOperation>();
        // Set the name field.
        updateOperations.Add(new SetUpdateOperation<string>("isDeleted", "true"));
        
        for (int i = 0; i < numberOfBatches; i++)
        {
            // Generate update items.

            List<UpdateItem> updateItemsInBatch = new List<UpdateItem>();

            for (int j = 0; j < numberOfDocumentsPerBatch; j++)
            {
                string partitionKeyValue = "12345";
                string id = listIds[0];
                listIds.RemoveAt(0);

                updateItemsInBatch.Add(new UpdateItem(id, partitionKeyValue, updateOperations));
            }

            // Invoke bulk update API.

            var tasks = new List<Task>();

            tasks.Add(Task.Run(async () =>
            {
                do
                {
                    try
                    {
                        bulkUpdateResponse = await bulkExecutor.BulkUpdateAsync(
                            updateItems: updateItemsInBatch,
                            maxConcurrencyPerPartitionKeyRange: null,
                            cancellationToken: token);
                    }
                    catch (DocumentClientException de)
                    {
                        Trace.TraceError("Document client exception: {0}", de);
                        break;
                    }
                    catch (Exception e)
                    {
                        Trace.TraceError("Exception: {0}", e);
                        break;
                    }
                } while (bulkUpdateResponse.NumberOfDocumentsUpdated < updateItemsInBatch.Count);

                totalNumberOfDocumentsUpdated += bulkUpdateResponse.NumberOfDocumentsUpdated;
                totalRequestUnitsConsumed += bulkUpdateResponse.TotalRequestUnitsConsumed;
                totalTimeTakenSec += bulkUpdateResponse.TotalTimeTaken.TotalSeconds;
            },
            token));

            await Task.WhenAll(tasks);
        }

        Console.WriteLine(String.Format("Updated {0} docs @ {1} update/s, {2} RU/s in {3} sec",
            totalNumberOfDocumentsUpdated,
            Math.Round(totalNumberOfDocumentsUpdated / totalTimeTakenSec),
            Math.Round(totalRequestUnitsConsumed / totalTimeTakenSec),
            totalTimeTakenSec));
        Console.WriteLine(String.Format("Average RU consumption per document update: {0}",
            (totalRequestUnitsConsumed / totalNumberOfDocumentsUpdated)));
        Console.WriteLine("TotalRUsConsumed: " + totalRequestUnitsConsumed);
    }
}
