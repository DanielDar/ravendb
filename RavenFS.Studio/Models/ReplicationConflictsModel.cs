﻿using System;
using System.Net;
using System.Reactive.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using RavenFS.Studio.Infrastructure;

namespace RavenFS.Studio.Models
{
    public class ReplicationConflictsModel : ViewModel
    {
        public VirtualCollection<FileSystemModel> ConflictedFiles { get; private set; }

        public ReplicationConflictsModel()
        {
            ConflictedFiles =
                new VirtualCollection<FileSystemModel>(
                    new SearchResultsCollectionSource() {SearchPattern = "Raven-Synchronization-Conflict:True"}, 30, 30);
        }

        protected override void OnViewLoaded()
        {
            ConflictedFiles.Refresh();

            ApplicationModel.Current.Client.Notifications.ConflictDetected()
                .Throttle(TimeSpan.FromSeconds(1))
                .TakeUntil(Unloaded)
                .ObserveOnDispatcher()
                .Subscribe(notification => ConflictedFiles.Refresh(RefreshMode.PermitStaleDataWhilstRefreshing));
        }
    }
}
