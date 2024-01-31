# A parser for InnoDB file formats, in Python

This library is written to acheive a better understanding of the articles from 
jeremycole's blog[<sup>[1]</sup>](#r1) about innodb.

As jeremycole say:
> What better way to improve your understanding of a structure than to implement it in another language?

So I write it during my learning. 
Jeremycole also write a parser in ruby[<sup>[2]</sup>](#r2), which is not avaliable in mysql 8.0. Compared with it, this library is based on mysql 8.0.

# Usage
```bash
python -m pyinnodb.cli --fn /opt/homebrew/var/mysql/test/t1.ibd static-page-usage

## Many other thing to learn
```
# Reference
<div id='r1'></div>

- [1] [Innodb Blog](https://blog.jcole.us/innodb/)
<div id='r2'></div>

- [2] [A parser for InnoDB file formats, in Ruby](https://github.com/jeremycole/innodb_ruby)
<div id='r3'></div>

- [3] [InnoDB Diagrams](https://github.com/jeremycole/innodb_diagrams)