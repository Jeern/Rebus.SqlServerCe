using System;

namespace Rebus.SqlServerCe
{
    /// <summary>
    /// Represents a table name in SQL Server Compact
    /// </summary>
    public class TableName : IEquatable<TableName>
    {
        /// <summary>
        /// Gets the table's name
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Creates a <see cref="TableName"/> object with the given table names
        /// </summary>
        public TableName(string tableName)
        {
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            Name = StripBrackets(tableName);
        }

        static string StripBrackets(string value)
        {
            if (value.StartsWith("["))
            {
                value = value.Substring(1);
            }
            if (value.EndsWith("]"))
            {
                value = value.Substring(0, value.Length - 1);
            }

            return value;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Name;
        }

        /// <inheritdoc />
        public bool Equals(TableName other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name, StringComparison.CurrentCultureIgnoreCase);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TableName) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        /// <summary>
        /// Checks whether the two <see cref="TableName"/> objects are equal (i.e. represent the same table)
        /// </summary>
        public static bool operator ==(TableName left, TableName right)
        {
            return Equals(left, right);
        }

        /// <summary>
        /// Checks whether the two <see cref="TableName"/> objects are not equal (i.e. do not represent the same table)
        /// </summary>
        public static bool operator !=(TableName left, TableName right)
        {
            return !Equals(left, right);
        }
    }
}
