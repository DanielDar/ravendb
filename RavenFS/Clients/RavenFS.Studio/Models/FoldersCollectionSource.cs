﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using RavenFS.Studio.Infrastructure;
using RavenFS.Studio.Extensions;

namespace RavenFS.Studio.Models
{
    public class FoldersCollectionSource : IVirtualCollectionSource<FileSystemModel>
    {
        private const int MaximumNumberOfFolders = 1024;
        private readonly object _lock = new object();
        private IList<FileSystemModel> folders;
        private IList<FileSystemModel> virtualFolders; 
        private string currentFolder;
        private readonly TaskScheduler synchronizationContextScheduler = TaskScheduler.FromCurrentSynchronizationContext();
        private bool isPruningFolders;
        private string searchPattern;
        private string searchPatternRegEx;
        private IList<FileSystemModel> combinedFilteredList = new List<FileSystemModel>();
        private int? count;

        public FoldersCollectionSource()
        {
            ApplicationModel.Current.State.VirtualFolders.VirtualFolders
                .ObserveCollectionChanged()
                .SubscribeWeakly(this, (t, e) => t.HandleVirtualFoldersChanged(e));    
        }

        private void HandleVirtualFoldersChanged(NotifyCollectionChangedEventArgs e)
        {
	        if (isPruningFolders) 
				return;

	        UpdateVirtualFolders();
	        Refresh(RefreshMode.PermitStaleDataWhilstRefreshing);
        }

        private void UpdateVirtualFolders()
        {
            lock (_lock)
            {
                virtualFolders =
                    ApplicationModel.Current.State.VirtualFolders.GetSubFolders(currentFolder).Cast<FileSystemModel>().ToList();
                UpdateCombinedFilteredList();
            }
        }

        private void UpdateCombinedFilteredList()
        {
            int folderCount;
            lock (_lock)
            {
                combinedFilteredList.Clear();

                var items = virtualFolders.EmptyIfNull().Concat(folders.EmptyIfNull()).Where(f => MatchesSearchPattern(f.Name));

                combinedFilteredList.AddRange(items);
                folderCount = combinedFilteredList.Count;
            }

            Count = folderCount;
        }

        private bool MatchesSearchPattern(string name)
        {
	        return string.IsNullOrEmpty(searchPatternRegEx) || Regex.IsMatch(name, searchPatternRegEx, RegexOptions.IgnoreCase);
        }

	    public string CurrentFolder
        {
            get { return currentFolder; }
            set
            {
                currentFolder = value;
                SetFolders(null);
                UpdateVirtualFolders();
                BeginGetFolders();
                OnCollectionChanged(new VirtualCollectionSourceChangedEventArgs(ChangeType.Reset));
            }
        }

        public string SearchPattern
        {
            get {
                return searchPattern;
            }
            set
            {
                if (searchPattern == value)
                {
                    return;
                }
                searchPattern = value;
                searchPatternRegEx = string.IsNullOrEmpty(searchPattern) ? "" : WildcardToRegex(searchPattern);
                UpdateCombinedFilteredList();
                Refresh(RefreshMode.ClearStaleData);
            }
        }

        public static string WildcardToRegex(string pattern)
        {
            return "^" + Regex.Escape(pattern).
            Replace("\\*", ".*").
            Replace("\\?", ".") + "$";
        }


        public Task<IList<FileSystemModel>> GetPageAsync(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
            lock (_lock)
            {
                if (folders == null && virtualFolders == null)
                {
                    return TaskEx.FromResult((IList<FileSystemModel>) (new FileSystemModel[0]));
                }

                var count = Math.Min(Math.Max(Count.Value- start,0), pageSize);

                return TaskEx.FromResult((IList<FileSystemModel>)(combinedFilteredList.Apply(e => ApplySort(e, sortDescriptions)).Skip(start).Take(count).ToArray()));
            }
        }

        private IEnumerable<FileSystemModel> ApplySort(IEnumerable<FileSystemModel> files, IList<SortDescription> sortDescriptions)
        {
            var sort = sortDescriptions.FirstOrDefault();

	        if (sort.PropertyName == "Name" && sort.Direction == ListSortDirection.Ascending)
		        return files.OrderBy(f => f.Name);
	        
			if (sort.PropertyName == "Name" && sort.Direction == ListSortDirection.Descending)
		        return files.OrderByDescending(f => f.Name);
	        
			return files;
        }

        private void BeginGetFolders()
        {
            if (string.IsNullOrEmpty(currentFolder))
            {
                return;
            }

            ApplicationModel.Current.Client.GetFoldersAsync(currentFolder, start: 0, pageSize: MaximumNumberOfFolders)
                .ContinueWith(
                    t =>
                        {
                            if (!t.IsFaulted)
                            {
                                var folders = t.Result.Select(n => new DirectoryModel {FullPath = n}).ToArray();
                                PruneVirtualFolders(folders);
                                SetFolders(folders);
                            }
                            else
                            {
                                SetFolders(new DirectoryModel[0]);
                            }

                            OnCollectionChanged(new VirtualCollectionSourceChangedEventArgs(ChangeType.Refresh));

                        }, synchronizationContextScheduler)
                        .Catch("Could not get folders from server");
        }

        private void PruneVirtualFolders(DirectoryModel[] folders)
        {
            isPruningFolders = true;
            ApplicationModel.Current.State.VirtualFolders.PruneFoldersThatNowExist(folders);
            isPruningFolders = false;

            UpdateVirtualFolders();
        }

        private void SetFolders(DirectoryModel[] folders)
        {
            lock (_lock)
            {
                this.folders = folders;
            }

            UpdateCombinedFilteredList();
        }

        public event EventHandler<VirtualCollectionSourceChangedEventArgs> CollectionChanged;

        protected virtual void OnCollectionChanged(VirtualCollectionSourceChangedEventArgs e)
        {
            var handler = CollectionChanged;
            if (handler != null) handler(this, e);
        }

        public event EventHandler<EventArgs> CountChanged;

        protected virtual void OnCountChanged()
        {
            var handler = CountChanged;
            if (handler != null) handler(this, EventArgs.Empty);
        }

        public int? Count
        {
            get { return count; }
            private set
            {
                bool wasChanged = false;
                lock (_lock)
                {
                    if (count != value)
                    {
                        count = value;
                        wasChanged = true;
                    }
                }

                if (wasChanged)
                {
                    OnCountChanged();
                }
            }
        }

        public void Refresh(RefreshMode mode)
        {
            BeginGetFolders();
        }
    }
}
