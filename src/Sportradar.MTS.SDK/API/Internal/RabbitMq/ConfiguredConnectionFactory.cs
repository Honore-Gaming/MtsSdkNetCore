/*
 * Copyright (C) Sportradar AG. See LICENSE for full license governing this code
 */
using System.Collections.Generic;
using Dawn;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Threading;
using RabbitMQ.Client;
using Microsoft.Extensions.Logging;
using Sportradar.MTS.SDK.Common;
using System;
using Sportradar.MTS.SDK.Common.Internal;
using Sportradar.MTS.SDK.Entities.Internal;
using System.Diagnostics;

namespace Sportradar.MTS.SDK.API.Internal.RabbitMq
{
    /// <summary>
    /// A <see cref="IConnectionFactory"/> implementations which properly configures it self before first <see cref="IConnection"/> is created
    /// </summary>
    internal class ConfiguredConnectionFactory
    {
        private static readonly ILogger ExecutionLog = SdkLoggerFactory.GetLogger(typeof(ConfiguredConnectionFactory));
        /// <summary>
        /// A <see cref="IRabbitServer"/> instance containing server information
        /// </summary>
        private readonly IRabbitServer _server;
        private static volatile int termninationCount = 0;

        /// <summary>
        /// A <see cref="ConnectionFactory"/> instance
        /// </summary>
        private readonly ConnectionFactory _connectionFactory;

        /// <summary>
        /// Value indicating whether the current <see cref="ConfiguredConnectionFactory"/> was already configured
        /// 0 indicates false; 1 indicates true
        /// </summary>
        private long _configured;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfiguredConnectionFactory"/> class
        /// </summary>
        /// <param name="server">A <see cref="IRabbitServer"/> instance containing server information</param>
        /// <param name="connectionFactory">A <see cref="ConnectionFactory"/> instance</param>
        public ConfiguredConnectionFactory(IRabbitServer server, ConnectionFactory connectionFactory)
        {
            Guard.Argument(server, nameof(server)).NotNull();

            _server = server;
            _connectionFactory = connectionFactory;
        }


        /// <summary>
        /// Configures the current <see cref="ConfiguredConnectionFactory"/> based on server options read from <code>_server</code> field
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Major Vulnerability", "S4423:Weak SSL/TLS protocols should not be used", Justification = "Need to support older for some clients")]
        private void Configure()
        {
            _connectionFactory.HostName = _server.HostAddress;
            _connectionFactory.Port = _server.Port;
            _connectionFactory.UserName = _server.Username;
            _connectionFactory.Password = _server.Password;
            _connectionFactory.VirtualHost = _server.VirtualHost;
            _connectionFactory.AutomaticRecoveryEnabled = _server.AutomaticRecovery;

            _connectionFactory.Ssl.Enabled = _server.UseSsl;
            _connectionFactory.Ssl.Version = SslProtocols.Tls12 | SslProtocols.Tls11 | SslProtocols.Tls;
            if (_server.UseSsl && ShouldUseCertificateValidation(_server.SslServerName))
            {
                _connectionFactory.Ssl.ServerName = _server.SslServerName;
            }
            else if (_server.UseSsl)
            {
                _connectionFactory.Ssl.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateNotAvailable;
            }

            if (_server.ClientProperties != null && _server.ClientProperties.Any())
            {
                _connectionFactory.ClientProperties = _server.ClientProperties as Dictionary<string, object>;
            }

            if (_server.HeartBeat >= 10)
            {
                _connectionFactory.RequestedHeartbeat = TimeSpan.FromSeconds(_server.HeartBeat);
            }
        }

        /// <summary>
        /// Create a connection to the specified endpoint.
        /// </summary>
        /// <exception cref="T:RabbitMQ.Client.Exceptions.BrokerUnreachableException">When the configured host name was not reachable</exception>
        public IConnection CreateConnection()
        {
            IConnection connection = null;
            try
            {
                if (Interlocked.CompareExchange(ref _configured, 1, 0) == 0)
                {
                    Configure();
                }
                connection = ExecuteWithTimeout(TimeSpan.FromMilliseconds(3000));
            }
            catch (Exception ex)
            {
                ExecutionLog.LogError(ex.Message, ex);
                ExecutionLog.LogWarning($"Error ConfiguredConnectionFactory.CreateConnection!");
            }
            return connection;
        }

        private static bool ShouldUseCertificateValidation(string hostName)
        {
            if (string.IsNullOrEmpty(hostName))
            {
                return true;
            }

            return true;
        }

        private IConnection CreateConnectionThread()
        {
            try
            {
                Thread thread = Thread.CurrentThread;
                thread.IsBackground = true;

                if (Interlocked.CompareExchange(ref _configured, 1, 0) == 0)
                {
                    Configure();
                }
                return this._connectionFactory.CreateConnection(GenerateConnectionName());
            }
            catch (Exception ex)
            {
                ExecutionLog.LogError(ex.Message, ex);
                ExecutionLog.LogWarning($"Error ConfiguredConnectionFactory.CreateConnection!");
            }
            return null;
        }

        private IConnection ExecuteWithTimeout(TimeSpan timeout)
        {
            IConnection connection = null;
            Thread createConnectionThread = new Thread(() => {
                connection = CreateConnectionThread();
            });
            
            bool finished = false;
            try
            {
                createConnectionThread.Start();
                finished = createConnectionThread.Join(timeout);
                if (!finished)
                {
                    createConnectionThread.Abort();
                    termninationCount++;
                    ExecutionLog.LogError($"Aborted ConfiguredConnectionFactory.CreateConnection number:{termninationCount}!");
                }
            }
            catch (Exception ex)
            {
                ExecutionLog.LogError(ex.Message, ex);
                ExecutionLog.LogWarning($"Error ConfiguredConnectionFactory.CreateConnection!");
            }

            return connection;
        }


        public string GenerateConnectionName()
        {
            var systemStartTime = DateTime.Now.AddMilliseconds(-Environment.TickCount);
            return $"MTS|NETStd|{SdkInfo.GetVersion()}|{DateTime.Now:yyyyMMddHHmm}|{TicketHelper.DateTimeToUnixTime(systemStartTime)}|{Process.GetCurrentProcess().Id}";
        }

    }
}
