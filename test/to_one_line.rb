module ToOneLine
  refine String do
    def to_one_line
      self.
        gsub(/\n/, ' ').
        gsub(/\s{2,}/, ' ').
        gsub(/^\s+/, '').
        gsub(/\s+$/, '').
        gsub(/> \[/, '>[')
    end
  end
end
