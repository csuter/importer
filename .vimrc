source ~/.myvim/jvimrc
source ~/.myvim/mvnvimrc
source ~/.myvim/svnvimrc

let g:projectBaseDir=''


"
" VSTreeExplore mappings
"
map <F3> :wtko
map <F4> :wtjo

"
" ant mappings
"
map <F5> :call RunAntScript("default", "src/lib")<CR>
map <F6> :call RunAntScript("all", "src/lib")<CR>
map <F7> :call RunAntScript("run", "src/lib")<CR>
map <F8> :call RunAntScript("run-hprof", "src/lib")<CR>
map <F9> :call RunAntScript("clean", "src/lib")<CR>

"
" mvn mappings
"map <F5> :!mvn compile<CR>
"map <F6> :!mvn install<CR>
"map <F7> :!mvn test<CR>
"map <F9> :!mvn clean<CR>



"
" Java mappings
" 
map <C-j>np :call NewConfigurableProperty()<CR>
map <C-j>al :call AddLogger()<CR>
map <C-j>no :call NewObject()<CR>
map <C-j>gs :call GettersAndSetters()<CR>
map <C-j>go :call GettersOnly()<CR>
map <C-j>si :call SortImports()<CR>
map <C-j>ns :call NewService()<CR>
map <C-j>nts :call NewService()<CR>
map <C-j>ac :call AddToConstructor()<CR>
map <C-j>rc :new ~/.myvim/jvimrc<CR>

      " insert comment
map <C-j>ic O/*  */hhi 

map <C-j>lc  :call ProjectLineCount()<CR>
map <C-j>xml :call AddXMLHeader()<CR>



"
" Maven mappings
"
map <C-m>pp :call AddPomProjectStartTag()<CR>




" 
" SVN mappings
"

map <C-c>c :call SVNcommit()<CR>
map <C-c>u :call SVNupdate()<CR>
map <C-c>r :call SVNcheckoutRevision()<CR>





"
" project specific stuff
"

function! NewConfigurableProperty()

	let name=input("Enter new property name: ")
	let default=input("Enter default value of " . name . ": ")

	" set mark on current spot
	execute 'normal mP'

	" open Configuration.java for editing
	execute 'e ' . g:projectBaseDir . '/src/main/com/datashark/config/Configuration.java'

	" go to beginning of file, find SettingNames, go to end brace under
	" SettingNames, and add new SettingName.
	execute 'normal gg/SettingNamesj^%kA,i' . name . ''

	" find static default-initialization block
	execute 'normal gg/^\tstatic\s*$j^%kodefaults.put(SettingNames.' . name . ', "' . default . '");:w'

	" return to initial mark
	execute 'normal `P'

endfunction

function! AddLogger()

	" ensure tidiness (at the top at least...)
	execute 'normal gg=Ggg'

	" add import statement
	execute 'normal joimport org.apache.log4j.Logger;'

	" copy class name to register n
	execute 'normal /^public\ \(\w\+\ \)\?class0/classW"nyE'

	" add logger declaration/initialization
	execute 'normal gg=G/^{ostatic Logger log = Logger.getLogger(n.class.getName());'

	" save
	execute 'normal :w'

endfunction

function! AddXMLHeader()
	execute 'normal maggO<?xml version="1.0" encoding="UTF-8"?>`a'
endfunction 

function! ProjectLineCount()
	execute '!clear; find . -name "*.java" | xargs cat | grep -v "^\s*$" | wc -l'
endfunction

function! NewService()
	execute 'normal /^{%'
	execute 'normal Opublic void do() throws Exception { }'
	execute 'normal yyPP'
	execute 'normal 0f(iInitj'
	execute 'normal 0f(iStartj'
	execute 'normal 0f(iStopj'
	execute 'normal :w``'
endfunction

function! NewThreadedService()
	execute 'normal /^{%'
	execute 'normal Opublic void do() throws Exception { }'
	execute 'normal yyPPP'
	execute 'normal 0f(iWorkj'
	execute 'normal 0f(iInitj'
	execute 'normal 0f(iStartj'
	execute 'normal 0f(iStopj'
	execute 'normal :w``'
endfunction

