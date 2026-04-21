# Setup the terminal appearance
Set Shell fish
Set FontSize 22
Set Width 1200
Set Height 600
Set Padding 20

Output vhs.gif

# 1. Run the scraper
Type "uv run scraper both --workers 30"
Enter
Sleep 60s

# 2. List the generated CSVs
Type "ls *.csv"
Sleep 500ms
Enter
Sleep 2s

# 3. Preview the output data
Type "head -n 3 output_khda.csv"
Sleep 500ms
Enter
Sleep 2s

Type "head -n 3 output_spea.csv"
Sleep 500ms
Enter

# Give the viewer a moment to see the results
Sleep 5s
