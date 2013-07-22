﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using RavenFS.Studio.Infrastructure;
using Validation = RavenFS.Studio.Infrastructure.Validation;

namespace RavenFS.Studio.Models
{
    public class EditableKeyValue : NotifyPropertyChangedBase, INotifyDataErrorInfo
    {
        public event EventHandler<DataErrorsChangedEventArgs> ErrorsChanged;

        private List<ValidationResult> validationErrors = new List<ValidationResult>();
        string key = string.Empty;
        string value = string.Empty;
        private bool isReadOnly;

        [RegularExpression(@"^[\w|-]*$", ErrorMessage = "Key must consist only of letters, digits, underscores and dashes")]
        public string Key
        {
            get { return key; }
            set
            {
                if (key != value)
                {
                    key = value;
                    OnPropertyChanged("Key");
                    Validate();
                }
            }
        }

        private void Validate()
        {
            Validation.Validate(this, validationErrors, property => OnErrorsChanged(new DataErrorsChangedEventArgs(property)));
        }

        public string Value
        {
            get { return value; }
            set
            {
                if (this.value != value)
                {
                    this.value = value;
                    OnPropertyChanged("Value");
                }
            }
        }

        public IEnumerable GetErrors(string propertyName)
        {
            return validationErrors.Where(e => e.MemberNames.Contains(propertyName));
        }

        public bool HasErrors
        {
            get { return validationErrors.Count > 0; }
        }

        public bool IsReadOnly
        {
            get 
            {
                return isReadOnly;
            }
            set 
            {
                isReadOnly = value;
                OnPropertyChanged("IsReadOnly");
            }
        }

        protected void OnErrorsChanged(DataErrorsChangedEventArgs e)
        {
            var handler = ErrorsChanged;
            if (handler != null) handler(this, e);
        }
    }
}
