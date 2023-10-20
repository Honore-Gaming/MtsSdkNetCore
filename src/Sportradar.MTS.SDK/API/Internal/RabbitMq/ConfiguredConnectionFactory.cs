/*
 * Copyright (C) Sportradar AG. See LICENSE for full license governing this code
 */

using System;
using System.Collections.Generic;
using Dawn;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Threading;
using RabbitMQ.Client;

namespace Sportradar.MTS.SDK.API.Internal.RabbitMq
{
    /// <summary>
    /// A <see cref="IConnectionFactory"/> implementations which properly configures it self before first <see cref="IConnection"/> is created
    /// </summary>
    internal class ConfiguredConnectionFactory
    {
        /// <summary>
        /// A <see cref="IRabbitServer"/> instance containing server information
        /// </summary>
        private readonly IRabbitServer _server;

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
            if (Interlocked.CompareExchange(ref _configured, 1, 0) == 0)
            {
                Configure();
            }
            return this._connectionFactory.CreateConnection();
        }

        private static bool ShouldUseCertificateValidation(string hostName)
        {
            if (string.IsNullOrEmpty(hostName))
            {
                return true;
            }

            return true;
        }
    }
}
