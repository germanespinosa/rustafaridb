 clear
 While (1)
 {
	 if ((get-itemproperty ./*.rs | where {$_.Mode -like "*a*"}).Length -gt 0)
	 {
		del ..\data\Collection*.dat
	    clear
		cargo test
		attrib -A > null
	 }
	 sleep 1
 }
 