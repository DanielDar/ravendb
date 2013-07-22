﻿using System;
using System.Diagnostics;
using System.Windows;
using System.Threading.Tasks;
using RavenFS.Client;
using RavenFS.Studio.Models;

namespace RavenFS.Studio.Infrastructure
{
	public static class InvocationExtensions
	{
		public static Action ViaCurrentDispatcher(this Action action)
		{
			var dispatcher = Deployment.Current.Dispatcher;
			return () =>
			{
				if (dispatcher.CheckAccess())
					action();
				dispatcher.InvokeAsync(action);
			};
		}

		public static Action<T> ViaCurrentDispatcher<T>(this Action<T> action)
		{
			var dispatcher = Deployment.Current.Dispatcher;
			return t =>
			{
				if (dispatcher.CheckAccess())
					action(t);
				dispatcher.InvokeAsync(() => action(t));
			};
		}

		public static Task ContinueOnSuccess<T>(this Task<T> parent, Action<T> action)
		{
			return parent.ContinueWith(task => action(task.Result));
		}

		public static Task<bool> ContinueWhenTrue(this Task<bool> parent, Action action)
		{
			return parent.ContinueWith(task =>
			{
				if (task.Result == false)
					return false;
				action();
				return true;
			});
		}

		public static Task<bool> ContinueWhenTrueInTheUIThread(this Task<bool> parent, Action action)
		{
			return parent.ContinueWhenTrue(() =>
			{
				if (Deployment.Current.Dispatcher.CheckAccess())
					action();
				Deployment.Current.Dispatcher.InvokeAsync(action)
					.Catch();
			});
		}

		public static Task<TResult> ContinueOnSuccess<T, TResult>(this Task<T> parent, Func<T, TResult> action)
		{
			return parent.ContinueWith(task => action(task.Result));
		}

		public static Task ContinueOnSuccess(this Task parent, Action action)
		{
			return parent.ContinueWith(task =>
			{
				if (task.IsFaulted)
					return task;

				return TaskEx.Run(action);
			}).Unwrap();
		}

        public static Task UpdateOperationWithOutcome(this Task parent, AsyncOperationModel operation)
        {
            return parent.ContinueOnUIThread(
                task =>
                    {
                        if (task.IsFaulted)
                        {
                            operation.Faulted(task.Exception);
                        }
                        else
                        {
                            operation.Completed();
                        }
                    });
        }

        public static Task ContinueOnUIThread(this Task parent, Action<Task> action)
        {
            return parent.ContinueWith(
                action,
                Schedulers.UIThread);
        }

        public static Task ContinueOnUIThread<TResult>(this Task<TResult> parent, Action<Task<TResult>> action)
        {
            return parent.ContinueWith(
                action,
                Schedulers.UIThread);
        }

	    public static Task ContinueOnSuccessInTheUIThread(this Task parent, Action action)
		{
			return parent.ContinueOnSuccess(() =>
			{
				if (Deployment.Current.Dispatcher.CheckAccess())
					action();
				Deployment.Current.Dispatcher.InvokeAsync(action)
					.Catch();
			});
		}

		public static Task ContinueOnSuccess(this Task parent, Func<Task> action)
		{
			return parent.ContinueWith(task =>
			{
				if (task.IsFaulted)
					return task;

				return action();
			}).Unwrap();
		}

		public static Task Finally(this Task task, Action action)
		{
			task.ContinueWith(t => action());
			return task;
		}

        public static Task<TResult> Catch<TResult>(this Task<TResult> parent, string message = null)
        {
            return parent.Catch(e => { }, message);
        }

        public static Task<TResult> Catch<TResult>(this Task<TResult> parent, Action<AggregateException> action, string message = null)
        {
            var stackTrace = new StackTrace();
            return parent.ContinueWith(task =>
            {
                if (task.IsFaulted == false)
                    return task;

                var ex = task.Exception.ExtractSingleInnerException();
                Execute.OnTheUI(() => ApplicationModel.Current.AddErrorNotification(ex, message, stackTrace))
                    .ContinueWith(_ => action(task.Exception));
                return task;
            }).Unwrap();
        }

		public static Task Catch(this Task parent, string message = null)
		{
			return parent.Catch(e => { }, message);
		}


        public static Task Catch(this Task parent, Action<AggregateException> action, string message = null)
		{
            var stackTrace = new StackTrace();
            return parent.ContinueWith(task =>
            {
                if (task.IsFaulted == false)
                    return task;

                var ex = task.Exception.ExtractSingleInnerException();
                Execute.OnTheUI(() => ApplicationModel.Current.AddErrorNotification(ex, message, stackTrace))
                    .ContinueWith(_ => action(task.Exception));
                return task;
            }).Unwrap();
		}

		public static Task CatchIgnore<TException>(this Task parent) where TException : Exception
		{
			return parent.CatchIgnore<TException>(() => { });
		}

		public static Task CatchIgnore<TException>(this Task parent, Action action) where TException : Exception
		{
			parent.ContinueWith(task =>
			{
				if (task.IsFaulted == false)
					return;

				if (task.Exception.ExtractSingleInnerException() is TException == false)
					return;

				task.Exception.Handle(exception => true);
				action();
			});

			return parent;
		}
	}
}