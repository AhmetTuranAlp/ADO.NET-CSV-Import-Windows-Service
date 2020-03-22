using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvReadService.Model
{
   public class SegmentVM
    {
        public int Id { get; set; }
        public string TcNo { get; set; }
        public string SegmentName { get; set; }
        public int Oncelik { get; set; }
    }
}
