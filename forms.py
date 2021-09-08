from wtforms import Form, FileField, validators

class UploadFileForm(Form):
    file = FileField("file", validators=[validators.DataRequired()])