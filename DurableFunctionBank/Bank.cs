using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace DurableFunctionBank
{
    public static class Bank
    {
        [FunctionName("Account")]
        public static void Account(
            [EntityTrigger] IDurableEntityContext ctx)
        {
            int currentValue = ctx.GetState<int>();
            int operand;

            if (ctx.IsNewlyConstructed)
            {
                currentValue = 0;
                ctx.SetState(currentValue);
            }

            switch (ctx.OperationName)
            {
                case "balance":              
                    break;
                case "deposit":
                    operand = ctx.GetInput<int>();
                    currentValue += operand;
                    ctx.SetState(currentValue);
                    break;
                case "withdraw":
                    operand = ctx.GetInput<int>();
                    currentValue -= operand;
                    ctx.SetState(currentValue);
                    break;
            }

            ctx.Return(currentValue);
        }

        [FunctionName("GetBalance")]
        public static async Task<int> GetBalance(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var accountId = context.GetInput<string>();

            EntityId id = new EntityId(nameof(Account), accountId);

            return await context.CallEntityAsync<int>(id, "balance");
        }

        [FunctionName("BalanceInquiry")]
        public static async Task<HttpResponseMessage> BalanceInquiry(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "Account/{accountId}")] HttpRequestMessage req,
            string accountId,
            [OrchestrationClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string instanceId; 

            // GET request
            if (req.Method == HttpMethod.Get)
            {
                instanceId = await starter.StartNewAsync("GetBalance", accountId);
                log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
                return await starter.WaitForCompletionOrCreateCheckStatusResponseAsync(req, instanceId, System.TimeSpan.MaxValue);
            }
            else
            {
                return req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            }
        }

        public class AccountOperation
        {
            public string AccountId { get; set; }

            public string Type { get; set; }

            public int Amount { get; set; }

            public AccountOperation(string accountId, string type, int amount)
            {
                AccountId = accountId;
                Type = type;
                Amount = amount;
            }
        }

        [FunctionName("Deposit")]
        public static async Task<string> Deposit(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var accountOperation = context.GetInput<AccountOperation>();

            EntityId id = new EntityId(nameof(Account), accountOperation.AccountId);

            return await context.CallEntityAsync<string>(id, "deposit", accountOperation.Amount);
        }

        [FunctionName("PerformDeposit")]
        public static async Task<HttpResponseMessage> PerformDeposit(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "Account/{accountId}/Deposit/{amount}")] HttpRequestMessage req,
            string accountId,
            string amount,
            [OrchestrationClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string instanceId;

            // POST request
            if (req.Method == HttpMethod.Post)
            {
                instanceId = await starter.StartNewAsync("Deposit", new AccountOperation(accountId, "deposit", Int32.Parse(amount)));
                log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
                return await starter.WaitForCompletionOrCreateCheckStatusResponseAsync(req, instanceId, System.TimeSpan.MaxValue);
            }
            else
            {
                return req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            }
        }

        [FunctionName("Withdraw")]
        public static async Task<string> Withdraw(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var accountOperation = context.GetInput<AccountOperation>();

            EntityId id = new EntityId(nameof(Account), accountOperation.AccountId);

            return await context.CallEntityAsync<string>(id, "withdraw", accountOperation.Amount);
        }

        [FunctionName("PerformWithdraw")]
        public static async Task<HttpResponseMessage> PerformWithdraw(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "Account/{accountId}/Withdraw/{amount}")] HttpRequestMessage req,
            string accountId,
            string amount,
            [OrchestrationClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string instanceId;

            // POST request
            if (req.Method == HttpMethod.Post)
            {
                instanceId = await starter.StartNewAsync("Withdraw", new AccountOperation(accountId, "withdraw", Int32.Parse(amount)));
                log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
                return await starter.WaitForCompletionOrCreateCheckStatusResponseAsync(req, instanceId, System.TimeSpan.MaxValue);
            }
            else
            {
                return req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            }
        }

        public class TransferOperation
        {
            public string FromAccountId { get; set; }

            public string ToAccountId { get; set; }

            public int Amount { get; set; }

            public TransferOperation(string fromAccountId, string toAccountId, int amount)
            {
                FromAccountId = fromAccountId;
                ToAccountId = toAccountId;
                Amount = amount;
            }
        }

        [FunctionName("Transfer")]
        public static async Task<bool> Transfer(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var transferOperation = context.GetInput<TransferOperation>();

            bool transferSuccessful = false;

            EntityId fromAccountId = new EntityId(nameof(Account), transferOperation.FromAccountId);
            EntityId toAccountId = new EntityId(nameof(Account), transferOperation.ToAccountId);

            using (await context.LockAsync(fromAccountId, toAccountId))
            {
                var fromAccountBalance = await context.CallEntityAsync<int>(fromAccountId, "balance");

                if(fromAccountBalance >= transferOperation.Amount)
                {
                    var taskList = new List<Task>();
                    taskList.Add(context.CallEntityAsync<int>(fromAccountId, "withdraw", transferOperation.Amount));
                    taskList.Add(context.CallEntityAsync<int>(toAccountId, "deposit", transferOperation.Amount));
                    await Task.WhenAll(taskList.ToArray());
                    transferSuccessful = true;
                }
            }

            return transferSuccessful;
        }

        [FunctionName("PerformTransfer")]
        public static async Task<HttpResponseMessage> PerformTransfer(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "Transfer/{amount}/From/{fromAccountId}/To/{toAccountId}")] HttpRequestMessage req,
            string amount,
            string fromAccountId,
            string toAccountId,
            [OrchestrationClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string instanceId;

            // POST request
            if (req.Method == HttpMethod.Post)
            {
                instanceId = await starter.StartNewAsync("Transfer", new TransferOperation(fromAccountId, toAccountId, Int32.Parse(amount)));
                log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
                return await starter.WaitForCompletionOrCreateCheckStatusResponseAsync(req, instanceId, System.TimeSpan.MaxValue);
            }
            else
            {
                return req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            }
        }
    }
}