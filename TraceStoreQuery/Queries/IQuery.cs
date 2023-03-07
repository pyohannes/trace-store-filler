using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TraceStoreQuery
{
    internal interface IQuery
    {
        public string Name { get; }
        public string Description { get; }

        public void Run(int count);
    }
}
