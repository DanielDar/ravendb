﻿using System;
using System.Diagnostics;
using System.Globalization;
using System.Text.RegularExpressions;
using RavenFS.Studio.Infrastructure;
using RavenFS.Studio.Extensions;

namespace RavenFS.Studio.Features.Search.ClauseBuilders
{
    public class FileSizeRangeClauseBuilder : SearchClauseBuilder
    {
        private const string InputRegEx = @"^(\d+)\s*(\w*)$";

        public FileSizeRangeClauseBuilder()
        {
            Description = "File Size...";
        }

        public override ViewModel GetInputModel()
        {
            return new FileSizeRangeClauseModel();
        }

        public override string GetSearchClauseFromModel(ViewModel model)
        {
            var rangeModel = model as FileSizeRangeClauseModel;
            Debug.Assert(rangeModel != null);

            return string.Format("__size_numeric:[{0} TO {1}]", 
                ConvertInputStringToRangeValue(rangeModel.LowerLimit),
                ConvertInputStringToRangeValue(rangeModel.UpperLimit));
        }

        private string ConvertInputStringToRangeValue(string input)
        {
	        if (input.IsNullOrEmpty())
		        return "*";

	        var match = Regex.Match(input, InputRegEx);
	        if (!match.Success)
		        return "*";

	        var value = long.Parse(match.Groups[1].Value);
            var multiplier = GetMultiplier(match.Groups[2].Value);

            value *= multiplier;

            return value.ToString(CultureInfo.InvariantCulture);
        }

        private long GetMultiplier(string value)
        {
	        if (value.IsNullOrEmpty())
		        return 1;

	        if (value.IndexOf("k", StringComparison.InvariantCultureIgnoreCase) > -1)
		        return 1024;
	        
			if (value.IndexOf("m", StringComparison.InvariantCultureIgnoreCase) > - 1)
		        return 1024*1024;

	        if (value.IndexOf("g", StringComparison.InvariantCultureIgnoreCase) > -1)
		        return 1024*1024*1024;
	        
			return 1;
        }
    }
}
