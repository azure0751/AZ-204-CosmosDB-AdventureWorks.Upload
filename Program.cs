namespace AdventureWorks.Upload
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Text.Json;
    using System.IO;
    using Microsoft.Azure.Cosmos;

    public class Program
    {
      

        static private int amountToInsert;
        static List<Model> models;

        static async Task Main(string[] args)
        {

            
            try
            {
                string EndpointUrl = "";
                string AuthorizationKey = "";
                string DatabaseName = "Retail";
                string ContainerName = "Online";
                string PrimaryKey = "/Category";
                string JsonFilePath = "models.json";

                Console.WriteLine($"Enter the Cosmos DB NoSQL endpoint");
                EndpointUrl = Console.ReadLine();

                Console.WriteLine($"{Environment.NewLine}Enter the Cosmos DB NoSQL endpoint Key");
                AuthorizationKey= Console.ReadLine();


                // <CreateClient>
                CosmosClient cosmosClient = new CosmosClient(EndpointUrl, AuthorizationKey, new CosmosClientOptions() { AllowBulkExecution = true });
                // </CreateClient>

                // <Initialize>
                Console.WriteLine($"{Environment.NewLine}Creating a database name {DatabaseName} if not already exists..{DatabaseName}");
                Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(DatabaseName);

                // Configure indexing policy to exclude all attributes to maximize RU/s usage
                Console.WriteLine($"{Environment.NewLine}Creating a container : {ContainerName} if not already exists...");
                Console.WriteLine($"{Environment.NewLine}Partition key is  : {PrimaryKey}.");

                await database.DefineContainer(ContainerName, PrimaryKey)
                        .WithIndexingPolicy()
                            .WithIndexingMode(IndexingMode.Consistent)
                            .WithIncludedPaths()
                                .Attach()
                            .WithExcludedPaths()
                                .Path("/*")
                                .Attach()
                        .Attach()
                    .CreateAsync();
                // </Initialize>

                Console.WriteLine($"{Environment.NewLine}Reading the local json file from ....{JsonFilePath}");


                using (StreamReader reader = new StreamReader(File.OpenRead(JsonFilePath)))
                {
                    string json = await reader.ReadToEndAsync();
                    models = JsonSerializer.Deserialize<List<Model>>(json);
                    amountToInsert = models.Count;
                }

                // Prepare items for insertion
                Console.WriteLine($"{Environment.NewLine}Preparing total {amountToInsert} items/documents to insert...");

                // Create the list of Tasks
                Console.WriteLine($"Starting...");
                Stopwatch stopwatch = Stopwatch.StartNew();
                // <ConcurrentTasks>
                Container container = database.GetContainer(ContainerName);

                List<Task> tasks = new List<Task>(amountToInsert);
                foreach (Model model in models)
                {
                    tasks.Add(container.CreateItemAsync(model, new PartitionKey(model.Category))
                        .ContinueWith(itemResponse =>
                        {
                            if (!itemResponse.IsCompletedSuccessfully)
                            {
                                AggregateException innerExceptions = itemResponse.Exception.Flatten();
                                if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                                {
                                    Console.WriteLine($"{Environment.NewLine}Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                                }
                                else
                                {
                                    Console.WriteLine($"{Environment.NewLine}Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                                }
                            }
                        }));
                }

                // Wait until all are done
                await Task.WhenAll(tasks);
                // </ConcurrentTasks>
                stopwatch.Stop();

                Console.WriteLine($"{Environment.NewLine}Finished writing {amountToInsert} items in {stopwatch.Elapsed}.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public class Model
        {
            public string id { get; set; }
            public string Name { get; set; }
            public string Category { get; set; }
            public string Description { get; set; }
            public string Photo { get; set; }
            public IList<Product> Products { get; set; }
        }

        public class Product
        {
            public string id { get; set; }
            public string Name { get; set; }
            public string Number { get; set; }
            public string Category { get; set; }
            public string Color { get; set; }
            public string Size { get; set; }
            public decimal? Weight { get; set; }
            public decimal ListPrice { get; set; }
            public string Photo { get; set; }
        }
    }
}