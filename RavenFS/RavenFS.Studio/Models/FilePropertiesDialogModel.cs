﻿using System.ComponentModel;
using System.Threading.Tasks;
using System.Windows.Input;
using RavenFS.Client;
using RavenFS.Extensions;
using RavenFS.Studio.Extensions;
using RavenFS.Studio.Infrastructure;
using System.Linq;
using RavenFS.Studio.Infrastructure.Input;

namespace RavenFS.Studio.Models
{
	public class FilePropertiesDialogModel : DialogModel
	{
        private EditableKeyValueCollection metadata;
	    private ICommand cancelCommand;
	    private ICommand saveCommand;
	    private EditableKeyValue emptyItem;
	    private ICommand deleteCommand;

        string statusMessage;

	    public FileModel File { get; set; }

		public FilePropertiesDialogModel()
		{
		}

        protected override void OnViewLoaded()
        {
            if (File != null)
            {
                StatusMessage = string.Format("Loading details for file '{0}'", File.Name);

                ApplicationModel.Current.Client.GetMetadataForAsync(File.FullPath)
                .ContinueOnUIThread(UpdateMetadata);
            }

            OnPropertyChanged("Title");
        }

	    public string Title
	    {
	        get { return string.Format("Edit properties for '{0}'", File.Name); }
	    }

		public EditableKeyValueCollection Metadata
		{
			get { return metadata; }
            private set
            {
                metadata = value;
                OnPropertyChanged(() => Metadata);
            }
		}

	    public bool IsSaveVisible
	    {
	        get { return Metadata != null; }
	    }

	    public string StatusMessage
	    {
	        get { return statusMessage; }
	        private set
	        {
	            statusMessage = value;
	            OnPropertyChanged("StatusMessage");
	        }
	    }

        private void UpdateMetadata(Task<NameValueCollection> metadataTask)
        {
            if (!metadataTask.IsFaulted)
            {
                var collection = metadataTask.Result.FilterHeadersForViewing();

                var editableCollection =
                    new EditableKeyValueCollection(
                        collection.Select(key => new EditableKeyValue()
                                                     {
                                                         Key = key,
                                                         Value = collection[key],
                                                         IsReadOnly = MetadataExtensions.ReadOnlyHeaders.Contains(key)
                                                     }));

                Metadata = editableCollection;
                AddEmptyItem();
            }
            else
            {
                StatusMessage = "File details could not be loaded. \n\n" +
                                metadataTask.Exception.ExtractSingleInnerException().Message;
            }

            OnPropertyChanged("IsSaveVisible");
        }

	    private void AddEmptyItem()
	    {
		    if (emptyItem != null)
			    emptyItem.PropertyChanged -= HandleEmptyItemPropertyChanged;

		    emptyItem = new EditableKeyValue();
	        emptyItem.PropertyChanged += HandleEmptyItemPropertyChanged;

            Metadata.Add(emptyItem);
	    }

	    private void HandleEmptyItemPropertyChanged(object sender, PropertyChangedEventArgs e)
	    {
            // as soon as the user starts modifying the item, it's no longer new, so add a new empty item
		    if (!string.IsNullOrEmpty(emptyItem.Key) || !string.IsNullOrEmpty(emptyItem.Value))
			    AddEmptyItem();
	    }

        public ICommand DeleteMetadataItemCommand { get { return deleteCommand ?? (deleteCommand = new ActionCommand(HandleDelete)); } }

	    private void HandleDelete(object item)
	    {
	        var metaDataItem = item as EditableKeyValue;
		    if (metaDataItem == null || metaDataItem == emptyItem || metaDataItem.IsReadOnly)
			    return;

		    Metadata.Remove(metaDataItem);
	    }

	    public ICommand CancelCommand { get { return cancelCommand ?? (cancelCommand = new ActionCommand(() => Close(false))); } }

        public ICommand SaveCommand { get { return saveCommand ?? (saveCommand = new ActionCommand(HandleSave)); } }

	    private void HandleSave()
	    {
            if (Metadata.Any(e => e.HasErrors))
            {
                AskUser.AlertUser("Edit Properties", "Please correct the errors indicated in red before saving.");
                return;
            }

	        var newMetaData = Metadata
                .Where(i => i != emptyItem)
                .ToNameValueCollection()
                .FilterHeaders();

            ApplicationModel.Current.AsyncOperations.Do(() =>
                ApplicationModel.Current.Client.UpdateMetadataAsync(File.FullPath, newMetaData), "Updating properties for file " + File.Name);

	        Close(true);
	    }
	}
}