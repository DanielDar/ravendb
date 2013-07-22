﻿using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using RavenFS.Client.Connections;
using RavenFS.Client.Util;

namespace RavenFS.Client.Changes
{
	public class ServerNotifications : IServerNotifications, IObserver<string>, IDisposable
	{
		private readonly string url;
        private readonly AtomicDictionary<NotificationSubject> subjects = new AtomicDictionary<NotificationSubject>(StringComparer.InvariantCultureIgnoreCase);
        private readonly ConcurrentSet<Task> pendingConnectionTasks = new ConcurrentSet<Task>();
		private int reconnectAttemptsRemaining;
		private IDisposable connection;

		private static int connectionCounter;
		private readonly string id;
	    private Task connectionTask;
	    private object gate = new object();

		public ServerNotifications(string url)
		{
			id = Interlocked.Increment(ref connectionCounter) + "/" +
				 Base62Util.Base62Random();
			this.url = url;
		}

		private async Task EstablishConnection()
		{
			var request= (HttpWebRequest) WebRequest.Create(url + "/changes/events?id=" + id);
		    request.Method = "GET";

			while (true)
			{
				try
				{
					var result = await request.ServerPullAsync();
					reconnectAttemptsRemaining = 3; // after the first successful try, we will retry 3 times before giving up
					connection = (IDisposable)result;
					result.Subscribe(this);
					return;
				}
				catch (Exception)
				{
					if (reconnectAttemptsRemaining <= 0)
						throw;

					reconnectAttemptsRemaining--;
				}
			}
		}

	    public Task ConnectionTask
	    {
	        get
	        {
	            EnsureConnectionInitiated();
	            return connectionTask;
	        }
	    }

	    private void EnsureConnectionInitiated()
        {
		    if (connectionTask != null)
			    return;

		    lock(gate)
            {
	            if (connectionTask != null)
		            return;

	            connectionTask = EstablishConnection()
                .ObserveException();
            }
        }

		private async Task AfterConnection(Func<Task> action)
		{
			try
			{
				await ConnectionTask;
				await action();
			}
			catch (Exception)
			{
				
			}
		}

	    public Task WhenSubscriptionsActive()
	    {
	        return TaskEx.WhenAll(pendingConnectionTasks);
	    }

        public IObservable<ConfigChange> ConfigurationChanges()
        {
            EnsureConnectionInitiated();

            var observable = subjects.GetOrAdd("config", s => new NotificationSubject<ConfigChange>(
                                                               () => ConfigureConnection("watch-config"), 
                                                               () => ConfigureConnection("unwatch-config"), 
                                                               item => true));

            return (IObservable<ConfigChange>)observable;
        }

	    private async Task ConfigureConnection(string command, string value = "")
	    {
	        var afterConnection = AfterConnection(() => Send(command, value));

            pendingConnectionTasks.Add(afterConnection);
		    await afterConnection;
			pendingConnectionTasks.TryRemove(afterConnection);
	    }

	    public IObservable<ConflictNotification> Conflicts()
        {
            EnsureConnectionInitiated();

            var observable = subjects.GetOrAdd("conflicts", s => new NotificationSubject<ConflictNotification>(
                                                               () => ConfigureConnection("watch-conflicts"), 
                                                               () => ConfigureConnection("unwatch-conflicts"), 
                                                               item => true));

            return (IObservable<ConflictNotification>)observable;
        }

        public IObservable<FileChange> FolderChanges(string folder)
        {
            if (!folder.StartsWith("/"))
            {
                throw new ArgumentException("folder must start with /");
            }

            var canonicalisedFolder = folder.TrimStart('/');

            EnsureConnectionInitiated();

            var observable = subjects.GetOrAdd("folder/" + canonicalisedFolder, s => new NotificationSubject<FileChange>(
                                                               () => ConfigureConnection("watch-folder", folder), 
                                                               () => ConfigureConnection("unwatch-folder", folder), 
                                                               f => f.File.StartsWith(folder, StringComparison.InvariantCultureIgnoreCase)));

            return (IObservable<FileChange>)observable;
        }

        public IObservable<SynchronizationUpdate> SynchronizationUpdates()
        {
            EnsureConnectionInitiated();

            var observable = subjects.GetOrAdd("sync", s => new NotificationSubject<SynchronizationUpdate>(
                                                               () => ConfigureConnection("watch-sync"),
                                                               () => ConfigureConnection("unwatch-sync"),
                                                               x => true));

            return (IObservable<SynchronizationUpdate>)observable;
        }

		internal IObservable<UploadFailed> FailedUploads()
		{
			EnsureConnectionInitiated();

			var observable = subjects.GetOrAdd("cancellations", s => new NotificationSubject<UploadFailed>(
															   () => ConfigureConnection("watch-cancellations"),
															   () => ConfigureConnection("unwatch-cancellations"),
															   x => true));

			return (IObservable<UploadFailed>)observable;
		}

		private Task Send(string command, string value)
		{
			try
			{
				var sendUrl = url + "/changes/config?id=" + id + "&command=" + command;
				if (string.IsNullOrEmpty(value) == false)
					sendUrl += "&value=" + Uri.EscapeUriString(value);

                var request = (HttpWebRequest)WebRequest.Create(sendUrl);
                request.Method = "GET";
				return request.GetResponseAsync().ObserveException();
			}
			catch (Exception e)
			{
				return Util.TaskExtensions.FromException(e).ObserveException();
			}
		}

		public void Dispose()
		{
			if (disposed)
				return;

			DisposeAsync().Wait();
		}

		private bool disposed;

		public async Task DisposeAsync()
		{
			if (disposed)
			{
				await TaskEx.FromResult(true);
				return;
			}
			disposed = true;
			reconnectAttemptsRemaining = 0;

			if (connection == null)
			{
				await TaskEx.FromResult(true);
				return;
			}

			foreach (var subject in subjects)
			{
				subject.Value.OnCompleted();
			}

			await Send("disconnect", null);

			try
			{
				connection.Dispose();
			}
			catch (Exception)
			{
			}
		}

		public void OnNext(string dataFromConnection)
        {
            var notification = NotificationJSonUtilities.Parse<Notification>(dataFromConnection);

            if (notification is Heartbeat)
            {
                return;
            }

            foreach (var subject in subjects)
            {
                subject.Value.OnNext(notification);
            }

        }

		public void OnError(Exception error)
		{
			if (reconnectAttemptsRemaining <= 0)
				return;

			try
			{
				EstablishConnection().ObserveException().Wait();
			}
			catch (Exception exception)
			{
				foreach (var subject in subjects)
				{
					subject.Value.OnError(exception);
				}
				subjects.Clear();
			}
		}

		public void OnCompleted()
		{
		}
	}
}