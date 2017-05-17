This documentation requires [jekyll](https://jekyllrb.com/). 

##### Istall Jekyll
1. If you don't already have ruby gems, download rubygems by running `sudo apt-get install rubygems`
2. `gem install jekyll bundler`

##### Running the server 
Run command `jekyll serve` to serve at at localhost:4000
The port can be changed in `_config.yml` file 

This project uses [Jekyll Documentation theme](http://idratherbewriting.com/documentation-theme-jekyll/). Follow the link to find instructions on how to use it.

##### Home Page
The `index.md` file is the displayed as the landing page of the website.

##### Adding content to Documentation
1. To add a new page, create a new md file in pages directory. Use file `samplepage.md` as an example.
2. To add the page to the top navigation bar, open `_data\topnav.yml` and under items add these lines without the square brackets
```
 - title: [Title of the page]
     url: [permalink of the page you created, eg, /samplepage]
```