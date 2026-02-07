# dbt-lineage TUI Demo
Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 700
Set Theme "Dracula"
Set TypingSpeed 50ms
Set Padding 10

Output assets/tui-demo.gif

# 1. Launch TUI (~7s)
Type "dbt-lineage -i -p tests/fixtures/simple_project --include-tests --include-seeds --include-exposures"
Sleep 500ms
Enter
Sleep 2s

# 2. Navigate the graph (~3.5s)
Type "l"
Sleep 600ms
Type "l"
Sleep 600ms
Type "j"
Sleep 600ms
Type "k"
Sleep 600ms
Type "h"
Sleep 600ms

# 3. Search for a model (~2.5s)
Type "/"
Sleep 400ms
Type "orders"
Sleep 800ms
Enter
Sleep 800ms

# 4. Toggle node list panel (~2.3s)
Type "n"
Sleep 1.5s
Type "n"
Sleep 800ms

# 5. Path highlighting (~3.5s)
Type "l"
Sleep 400ms
Type "p"
Sleep 2s
Type "p"
Sleep 800ms

# 6. Zoom out and back in (~2.5s)
Type "----"
Sleep 1.5s
Type "++++"
Sleep 1s

# 7. Filter mode: hide tests (~3s)
Type "f"
Sleep 600ms
Type "t"
Sleep 1.5s
Escape
Sleep 800ms

# 8. Quit (~1s)
Type "q"
Sleep 1s
