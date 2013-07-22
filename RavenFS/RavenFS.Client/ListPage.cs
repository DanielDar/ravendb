﻿using System.Collections.Generic;
using System.Linq;

namespace RavenFS.Client
{
    public class ListPage<T>
    {
        public ListPage(IEnumerable<T> items, int total)
        {
            TotalCount = total;
            Items = items.ToList();
        }

        public int TotalCount { get; set; }
        public IList<T> Items { get; set; } 
    }
}
