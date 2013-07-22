﻿using System;
using RavenFS.Client;
using RavenFS.Studio.Extensions;
using RavenFS.Studio.Infrastructure;

namespace RavenFS.Studio.Models
{
    public class AsyncOperationModel : ViewModel
    {
        string progressText;
        string description;
        double progress;
        string error;
        Exception exception;
        AsyncOperationStatus status;

        public AsyncOperationModel()
        {
            Status = AsyncOperationStatus.Queued;
        }

        public string Description
        {
            get { return description; }
            set
            {
                description = value;
                OnPropertyChanged("Description");
            }
        }

        public double Progress
        {
            get { return progress; }
            private set
            {
                progress = value;
                OnPropertyChanged("Progress");
            }
        }

        public string ProgressText
        {
            get { return progressText; }
            set
            {
                progressText = value;
                OnPropertyChanged("ProgressText");
            }
        }

        public AsyncOperationStatus Status
        {
            get { return status; }
            private set
            {
                status = value;
                OnPropertyChanged("Status");
            }
        }

        public string Error
        {
            get { return error; }
            private set
            {
                error = value;
                OnPropertyChanged("Error");
            }
        }

        public Exception Exception
        {
            get { return exception; }
            set
            {
                exception = value;
                OnPropertyChanged("Exception");
            }
        }

        public void ProgressChanged(double amountCompleted, double amountToDo, string progressText = "")
        {
			if (Math.Abs(amountToDo - 0) < double.Epsilon)
				amountToDo = double.Epsilon;

            ProgressChanged((amountCompleted / amountToDo).Clamp(0, 1), progressText);
        }

        public void ProgressChanged(double progress, string progressText = "")
        {
	        if (progress < 0 || progress > 1)
		        throw new ArgumentOutOfRangeException("progress", "progress must be between 0 and 1");

	        if (Status == AsyncOperationStatus.Queued)
		        Started();

	        Progress = progress;
            ProgressText = progressText;
        }

        public void Started()
        {
            Status = AsyncOperationStatus.Processing;
        }

        public new void Completed()
        {
            Status = AsyncOperationStatus.Completed;
            Progress = 0;
            ProgressText = "";
        }

        public void Faulted(Exception exception)
        {
            Status = AsyncOperationStatus.Error;
            if (exception != null)
            {
                Exception = exception;

                Error = exception is AggregateException
                            ? ((exception as AggregateException).ExtractSingleInnerException() ?? exception).Message
                            : exception.Message;
            }

            Progress = 0;
        }
    }
}
